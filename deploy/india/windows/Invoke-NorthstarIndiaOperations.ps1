<#
.SYNOPSIS
    Runs one Northstar India futures operation for Windows Task Scheduler.

.DESCRIPTION
    Stage 1 of Windows scheduling for the India deployment (NIFTY / NSE /
    Upstox). It runs exactly the existing india-operations service -- the
    image's own `northstar operations daily` command -- once, through Docker
    Desktop, as the Linux systemd unit does:

        docker compose --project-directory <deployment directory>
            --file <deployment directory>\compose.yaml
            --project-name northstar-india
            run --rm -T --no-deps --name <this run's container> india-operations

    It adds only what Task Scheduler lacks: preflight checks, a bounded run,
    cleanup of the one container this run started, UTC-stamped per-run logs and
    an atomically replaced last-run.json.

    What it never does
    ------------------
    It never writes .env, never changes finality approval, never passes an
    environment override or a command to the container, and never runs any
    other Northstar command. It reads only two non-secret settings from .env
    (the finality mode and the final-through date) to log them. Finality stays
    exactly what .env says; the operation itself stops at the first session
    that is not yet final. Northstar's DatabaseOperationsLock remains the only
    database lock: an overlapping operation is reported by Northstar as
    SKIPPED and exits 0.

    Exit codes
    ----------
    0-6  Northstar's own exit code, passed through unchanged.
    10   Docker unavailable: the docker CLI, the Docker Desktop engine (in
         Linux-containers mode) or Docker Compose could not be used. Nothing ran.
    11   Deployment invalid: the deployment directory, compose.yaml or .env is
         missing, the Compose configuration does not validate, or the
         northstar-api:india image is not built. Nothing ran.
    12   Timeout: the operation exceeded -TimeoutSeconds. The container this run
         started was stopped and removed; no other container was touched.
    13   Wrapper error: the wrapper itself failed, for example because its log
         directory could not be written, or it was interrupted mid-run.

    Outcomes in last-run.json
    -------------------------
    RUNNING    written before the operation starts; still present afterwards
               only if the wrapper process was killed.
    COMPLETED  exit 0 and Northstar printed "STATUS: COMPLETED".
    WAITING    exit 0 and Northstar printed "STATUS: WAITING".
    SKIPPED    exit 0 and Northstar printed "DAILY OPERATION: SKIPPED" (another
               operations writer held the database lock).
    FAILED     any non-zero exit, or exit 0 without a recognised status line.

.PARAMETER DeploymentDirectory
    The India deployment directory holding compose.yaml and .env. Defaults to
    the parent of this script's directory.

.PARAMETER LogDirectory
    Where logs\<run id>.log and last-run.json are written. Defaults to
    %LOCALAPPDATA%\Northstar\india-operations of the account running the task.

.PARAMETER TimeoutSeconds
    Upper bound for the operation itself. Keep it below the scheduled task's
    execution time limit so the wrapper, not Task Scheduler, ends a stuck run.

.PARAMETER PreflightTimeoutSeconds
    Upper bound for each preflight docker command.

.PARAMETER LogRetentionDays
    Per-run logs older than this are deleted after each run; 0 keeps them all.

.PARAMETER DockerExecutable
    The docker CLI to call. Defaults to docker on PATH.

.PARAMETER PreflightOnly
    Run every preflight check, log the result and exit without running the
    operation. last-run.json is not changed.
#>
[CmdletBinding()]
param(
    [string] $DeploymentDirectory = (Join-Path $PSScriptRoot '..'),
    [string] $LogDirectory = (Join-Path $env:LOCALAPPDATA 'Northstar\india-operations'),
    [ValidateRange(1, 86400)] [int] $TimeoutSeconds = 2700,
    [ValidateRange(1, 3600)] [int] $PreflightTimeoutSeconds = 60,
    [ValidateRange(0, 36500)] [int] $LogRetentionDays = 90,
    [string] $DockerExecutable = 'docker',
    [switch] $PreflightOnly
)

Set-StrictMode -Version 2.0
$ErrorActionPreference = 'Stop'

$ProjectName = 'northstar-india'
$Service = 'india-operations'
$Image = 'northstar-api:india'
$StopGraceSeconds = 30
$StatusSchema = 'northstar.india-operations.last-run/1'
$ExitDockerUnavailable = 10
$ExitDeploymentInvalid = 11
$ExitTimeout = 12
$ExitWrapperError = 13
$ExitClasses = @{
    0 = 'SUCCESS'; 1 = 'INTERNAL'; 2 = 'INPUT'; 3 = 'CONFIGURATION'; 4 = 'DATA'; 5 = 'STATE'
    6 = 'PROVIDER'; 10 = 'DOCKER_UNAVAILABLE'; 11 = 'DEPLOYMENT_INVALID'; 12 = 'TIMEOUT'
    13 = 'WRAPPER_ERROR'
}
$LogNamePattern = '^\d{8}T\d{6}Z-[0-9a-f]{8}\.log$'
$Utf8 = New-Object System.Text.UTF8Encoding($false)
$Invariant = [System.Globalization.CultureInfo]::InvariantCulture

$StartedAt = [DateTime]::UtcNow
$RunId = $StartedAt.ToString("yyyyMMdd'T'HHmmss'Z'", $Invariant) + '-' +
    [Guid]::NewGuid().ToString('N').Substring(0, 8)
# Unique per invocation: cleanup can only ever address this run's container.
$ContainerName = "northstar-india-operations-$RunId".ToLowerInvariant()

$script:LogFile = $null
$script:LogsDirectory = $null
$script:Captured = $null

$Status = [ordered]@{
    schema              = $StatusSchema
    runId               = $RunId
    outcome             = 'RUNNING'
    reason              = $null
    exitCode            = $null
    exitClass           = $null
    northstarExitCode   = $null
    statusLine          = $null
    startedAtUtc        = $null
    endedAtUtc          = $null
    durationSeconds     = $null
    containerName       = $ContainerName
    finalityMode        = $null
    finalThrough        = $null
    dockerServerVersion = $null
    deploymentDirectory = $null
    logFile             = $null
    computer            = $env:COMPUTERNAME
}

function Format-Utc([DateTime] $Value) {
    return $Value.ToUniversalTime().ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'", $Invariant)
}

function Write-RunLog([string] $Level, [string] $Message) {
    $line = '{0} [{1}] {2} {3}' -f (Format-Utc ([DateTime]::UtcNow)), $RunId, $Level, $Message
    if ($script:LogFile) {
        [System.IO.File]::AppendAllText($script:LogFile, $line + "`r`n", $Utf8)
    }
    [Console]::Out.WriteLine($line)
}

function Write-RawLog([string] $Line) {
    if ($script:LogFile) {
        [System.IO.File]::AppendAllText($script:LogFile, $Line + "`r`n", $Utf8)
    }
    [Console]::Out.WriteLine($Line)
}

function ConvertTo-CommandLine([string[]] $Arguments) {
    # Windows argument quoting (CommandLineToArgvW rules), so paths with spaces,
    # parentheses or trailing backslashes reach the docker CLI exactly.
    $quoted = foreach ($argument in $Arguments) {
        if ($argument.Length -gt 0 -and $argument -notmatch '[\s"]') {
            $argument
            continue
        }
        $builder = New-Object System.Text.StringBuilder
        [void] $builder.Append('"')
        $backslashes = 0
        foreach ($character in $argument.ToCharArray()) {
            if ($character -eq [char] '\') {
                $backslashes++
                continue
            }
            if ($character -eq [char] '"') {
                [void] $builder.Append([char] '\', 2 * $backslashes + 1)
            } elseif ($backslashes -gt 0) {
                [void] $builder.Append([char] '\', $backslashes)
            }
            [void] $builder.Append($character)
            $backslashes = 0
        }
        if ($backslashes -gt 0) {
            [void] $builder.Append([char] '\', 2 * $backslashes)
        }
        [void] $builder.Append('"')
        $builder.ToString()
    }
    return ($quoted -join ' ')
}

function Start-CapturedProcess([string] $FilePath, [string[]] $Arguments) {
    $info = New-Object System.Diagnostics.ProcessStartInfo
    $info.FileName = $FilePath
    $info.Arguments = ConvertTo-CommandLine $Arguments
    $info.UseShellExecute = $false
    $info.CreateNoWindow = $true
    $info.RedirectStandardInput = $true
    $info.RedirectStandardOutput = $true
    $info.RedirectStandardError = $true
    $info.StandardOutputEncoding = $Utf8
    $info.StandardErrorEncoding = $Utf8
    $process = New-Object System.Diagnostics.Process
    $process.StartInfo = $info
    [void] $process.Start()
    # Unattended: nothing is ever typed into the operation.
    $process.StandardInput.Close()
    return [pscustomobject]@{
        Process = $process
        Stdout  = $process.StandardOutput.ReadToEndAsync()
        Stderr  = $process.StandardError.ReadToEndAsync()
    }
}

function Wait-CapturedProcess($Captured, [int] $Seconds) {
    if ($Captured.Process.WaitForExit($Seconds * 1000)) {
        $Captured.Process.WaitForExit()
        return $true
    }
    return $false
}

function Get-CapturedText($Captured) {
    $stdout = ''
    $stderr = ''
    if ($Captured.Stdout.Wait(10000)) { $stdout = $Captured.Stdout.Result }
    if ($Captured.Stderr.Wait(10000)) { $stderr = $Captured.Stderr.Result }
    return [pscustomobject]@{ Stdout = $stdout; Stderr = $stderr }
}

function Stop-ProcessTree([System.Diagnostics.Process] $Process) {
    if ($Process.HasExited) { return }
    # The whole tree: the docker CLI starts the Compose plugin as a child process.
    $taskkill = Join-Path $env:SystemRoot 'System32\taskkill.exe'
    $kill = Start-CapturedProcess $taskkill @('/PID', "$($Process.Id)", '/T', '/F')
    [void] (Wait-CapturedProcess $kill 30)
    [void] $Process.WaitForExit(15000)
}

function Invoke-Docker([string[]] $Arguments, [int] $Seconds) {
    $captured = Start-CapturedProcess $DockerExecutable $Arguments
    if (-not (Wait-CapturedProcess $captured $Seconds)) {
        Stop-ProcessTree $captured.Process
        return [pscustomobject]@{ TimedOut = $true; ExitCode = $null; Stdout = ''; Stderr = '' }
    }
    $text = Get-CapturedText $captured
    return [pscustomobject]@{
        TimedOut = $false
        ExitCode = $captured.Process.ExitCode
        Stdout   = $text.Stdout
        Stderr   = $text.Stderr
    }
}

function Get-FirstLine([string] $Text) {
    foreach ($line in ($Text -split "`r?`n")) {
        if ($line.Trim()) { return $line.Trim() }
    }
    return ''
}

function Read-EnvSetting([string[]] $Lines, [string] $Name) {
    $value = $null
    $pattern = '^\s*' + [regex]::Escape($Name) + '\s*=(.*)$'
    foreach ($line in $Lines) {
        if ($line -match $pattern) {
            $value = $Matches[1].Trim().Trim('"').Trim("'")
        }
    }
    return $value
}

function Set-Failure([int] $Code, [string] $Reason) {
    $Status.outcome = 'FAILED'
    $Status.reason = $Reason
    Write-RunLog 'ERROR' $Reason
    return $Code
}

function Stop-OwnContainer {
    # Only the container this invocation named. Never india-api, india-web,
    # another run's container or anything selected by a filter.
    $stop = Invoke-Docker @('stop', '--time', "$StopGraceSeconds", $ContainerName) `
        ($StopGraceSeconds + $PreflightTimeoutSeconds)
    if ($stop.TimedOut) {
        Write-RunLog 'WARN' "docker stop $ContainerName did not answer in time"
    } else {
        Write-RunLog 'WARN' "docker stop $ContainerName exited $($stop.ExitCode)"
    }
}

function Remove-OwnContainer {
    $remove = Invoke-Docker @('rm', '--force', $ContainerName) $PreflightTimeoutSeconds
    if (-not $remove.TimedOut -and $remove.ExitCode -eq 0) {
        Write-RunLog 'WARN' "removed container $ContainerName"
    } else {
        Write-RunLog 'INFO' "container $ContainerName was already gone or could not be removed"
    }
}

function Stop-OwnRun {
    Stop-OwnContainer
    Stop-ProcessTree $script:Captured.Process
    Remove-OwnContainer
}

function Write-CapturedOutput($Text) {
    Write-RunLog 'INFO' 'operation output follows (captured when the run ended; Northstar lines carry no timestamps of their own)'
    foreach ($line in ($Text.Stdout -split "`r?`n")) {
        if ($line) { Write-RawLog "    stdout | $line" }
    }
    foreach ($line in ($Text.Stderr -split "`r?`n")) {
        if ($line) { Write-RawLog "    stderr | $line" }
    }
}

function Write-StatusFile {
    $json = ConvertTo-Json -InputObject $Status -Depth 4
    $target = Join-Path $LogDirectory 'last-run.json'
    $temporary = Join-Path $LogDirectory "last-run.json.$RunId.tmp"
    [System.IO.File]::WriteAllText($temporary, $json + "`n", $Utf8)
    # Readers see either the previous complete file or this one, never a partial write.
    for ($attempt = 1; ; $attempt++) {
        try {
            if ([System.IO.File]::Exists($target)) {
                [System.IO.File]::Replace($temporary, $target, [NullString]::Value)
            } else {
                [System.IO.File]::Move($temporary, $target)
            }
            return
        } catch {
            if ($attempt -ge 50) { throw }
            Start-Sleep -Milliseconds 100
        }
    }
}

function Remove-ExpiredLogs {
    if ($LogRetentionDays -le 0 -or -not $script:LogsDirectory) { return }
    $cutoff = [DateTime]::UtcNow.AddDays(-$LogRetentionDays)
    Get-ChildItem -LiteralPath $script:LogsDirectory -File |
        Where-Object {
            $_.Name -match $LogNamePattern -and $_.LastWriteTimeUtc -lt $cutoff -and
                $_.FullName -ne $script:LogFile
        } |
        Remove-Item -Force
    Get-ChildItem -LiteralPath $LogDirectory -File -Filter 'last-run.json.*.tmp' |
        Where-Object { $_.LastWriteTimeUtc -lt $cutoff } |
        Remove-Item -Force
}

function Invoke-Operation {
    $deployment = [System.IO.Path]::GetFullPath($DeploymentDirectory)
    $composeFile = Join-Path $deployment 'compose.yaml'
    $envFile = Join-Path $deployment '.env'
    $Status.deploymentDirectory = $deployment

    Write-RunLog 'INFO' "run $RunId started by $env:USERDOMAIN\$env:USERNAME on $env:COMPUTERNAME"
    Write-RunLog 'INFO' "deployment directory: $deployment"
    Write-RunLog 'INFO' "container: $ContainerName; timeout: $TimeoutSeconds s; log retention: $LogRetentionDays day(s)"

    # 1. Deployment files. Nothing is read from .env except two non-secret settings.
    if (-not [System.IO.Directory]::Exists($deployment)) {
        return Set-Failure $ExitDeploymentInvalid "deployment directory not found: $deployment"
    }
    foreach ($required in @($composeFile, $envFile)) {
        if (-not [System.IO.File]::Exists($required)) {
            return Set-Failure $ExitDeploymentInvalid "required file not found: $required"
        }
    }
    $lines = [System.IO.File]::ReadAllLines($envFile)
    $mode = Read-EnvSetting $lines 'NORTHSTAR_FUTURES_DAILY_BAR_FINALITY'
    $through = Read-EnvSetting $lines 'NORTHSTAR_FUTURES_FINAL_THROUGH'
    $lines = $null
    if (-not $mode) {
        $Status.finalityMode = '<unset; compose default is disabled>'
    } elseif ($mode -eq 'disabled' -or $mode -eq 'operator-approved') {
        $Status.finalityMode = $mode
    } else {
        $Status.finalityMode = '<unrecognized>'
    }
    if (-not $through) {
        $Status.finalThrough = '<unset>'
    } elseif ($through -match '^\d{4}-\d{2}-\d{2}$') {
        $Status.finalThrough = $through
    } else {
        $Status.finalThrough = '<unrecognized>'
    }
    Write-RunLog 'INFO' ("finality mode {0}; final-through {1} (read from .env, never changed here)" -f `
        $Status.finalityMode, $Status.finalThrough)

    # 2. Docker Desktop: the CLI, a reachable engine in Linux-containers mode, Compose.
    try {
        $info = Invoke-Docker @('info', '--format', '{{.ServerVersion}} {{.OSType}}') $PreflightTimeoutSeconds
    } catch {
        return Set-Failure $ExitDockerUnavailable "docker CLI could not be started ($DockerExecutable): $($_.Exception.Message)"
    }
    if ($info.TimedOut) {
        return Set-Failure $ExitDockerUnavailable "docker info did not answer within $PreflightTimeoutSeconds s; is Docker Desktop running?"
    }
    if ($info.ExitCode -ne 0) {
        return Set-Failure $ExitDockerUnavailable ("Docker engine unavailable (docker info exited {0}): {1}" -f `
            $info.ExitCode, (Get-FirstLine $info.Stderr))
    }
    $engine = (Get-FirstLine $info.Stdout) -split ' '
    $Status.dockerServerVersion = $engine[0]
    if ($engine.Count -lt 2 -or $engine[1] -ne 'linux') {
        return Set-Failure $ExitDockerUnavailable "Docker engine is not in Linux-containers mode: $(Get-FirstLine $info.Stdout)"
    }
    $compose = Invoke-Docker @('compose', 'version', '--short') $PreflightTimeoutSeconds
    if ($compose.TimedOut -or $compose.ExitCode -ne 0) {
        return Set-Failure $ExitDockerUnavailable 'docker compose is not available'
    }
    Write-RunLog 'INFO' ("docker engine {0} (linux); compose {1}" -f $engine[0], (Get-FirstLine $compose.Stdout))

    # 3. The deployment itself: Compose interpolation of .env, and the built image.
    $composeBase = @(
        'compose', '--project-directory', $deployment, '--file', $composeFile,
        '--project-name', $ProjectName
    )
    $config = Invoke-Docker ($composeBase + @('config', '--quiet')) $PreflightTimeoutSeconds
    if ($config.TimedOut) {
        return Set-Failure $ExitDockerUnavailable "docker compose config did not answer within $PreflightTimeoutSeconds s"
    }
    if ($config.ExitCode -ne 0) {
        # Its message is not logged: Compose may quote .env values in it.
        return Set-Failure $ExitDeploymentInvalid (
            ("the Compose configuration does not validate (exit {0}); " -f $config.ExitCode) +
            "run 'docker compose config --quiet' in the deployment directory to see why"
        )
    }
    $imageCheck = Invoke-Docker @('image', 'inspect', '--format', '{{.Id}}', $Image) $PreflightTimeoutSeconds
    if ($imageCheck.TimedOut -or $imageCheck.ExitCode -ne 0) {
        return Set-Failure $ExitDeploymentInvalid "image $Image is not built on this host; build it with 'docker compose build'"
    }
    Write-RunLog 'INFO' 'preflight passed'
    if ($PreflightOnly) {
        $Status.outcome = 'PREFLIGHT_PASSED'
        Write-RunLog 'INFO' 'preflight only: the operation was not run and last-run.json was not changed'
        return 0
    }

    # 4. The operation: the service's own command, no override, no extra environment.
    Write-StatusFile
    $arguments = $composeBase + @('run', '--rm', '-T', '--no-deps', '--name', $ContainerName, $Service)
    Write-RunLog 'INFO' ("command: {0} {1}" -f $DockerExecutable, (ConvertTo-CommandLine $arguments))
    $script:Captured = Start-CapturedProcess $DockerExecutable $arguments
    if (-not (Wait-CapturedProcess $script:Captured $TimeoutSeconds)) {
        Write-RunLog 'ERROR' "the operation exceeded $TimeoutSeconds s; stopping this run's container only"
        Stop-OwnRun
        Write-CapturedOutput (Get-CapturedText $script:Captured)
        return Set-Failure $ExitTimeout ("timed out after {0} s; container {1} was stopped and removed" -f `
            $TimeoutSeconds, $ContainerName)
    }
    $text = Get-CapturedText $script:Captured
    Write-CapturedOutput $text
    $code = $script:Captured.Process.ExitCode
    $Status.northstarExitCode = $code

    $statusLine = ($text.Stdout -split "`r?`n") |
        Where-Object { $_ -match '^(STATUS: |DAILY OPERATION: SKIPPED)' } |
        Select-Object -Last 1
    $errorLine = ($text.Stderr -split "`r?`n") |
        Where-Object { $_ -match '^[A-Z]+ ERROR: ' } |
        Select-Object -First 1
    $Status.statusLine = $statusLine
    if ($code -ne 0) {
        $Status.outcome = 'FAILED'
        if ($errorLine) { $Status.reason = $errorLine } else { $Status.reason = "operation exited $code" }
    } elseif ($statusLine -match '^DAILY OPERATION: SKIPPED') {
        $Status.outcome = 'SKIPPED'
        $Status.reason = 'another operations writer held the database lock'
    } elseif ($statusLine -match '^STATUS: WAITING') {
        $Status.outcome = 'WAITING'
    } elseif ($statusLine -match '^STATUS: COMPLETED') {
        $Status.outcome = 'COMPLETED'
    } else {
        $Status.outcome = 'FAILED'
        $Status.reason = 'the operation exited 0 without a recognised status line'
    }
    return $code
}

$exitCode = $ExitWrapperError
try {
    $LogDirectory = [System.IO.Path]::GetFullPath($LogDirectory)
    $script:LogsDirectory = Join-Path $LogDirectory 'logs'
    [void] [System.IO.Directory]::CreateDirectory($script:LogsDirectory)
    $script:LogFile = Join-Path $script:LogsDirectory "$RunId.log"
    $Status.logFile = $script:LogFile
    $Status.startedAtUtc = Format-Utc $StartedAt
    # The exit code is the function's last output, whatever else reached the pipeline.
    $exitCode = [int] (Invoke-Operation | Select-Object -Last 1)
} catch {
    $Status.outcome = 'FAILED'
    $Status.reason = "wrapper error: $($_.Exception.Message)"
    $exitCode = $ExitWrapperError
    try { Write-RunLog 'ERROR' $Status.reason } catch { [Console]::Error.WriteLine($Status.reason) }
} finally {
    if ($null -ne $script:Captured -and -not $script:Captured.Process.HasExited) {
        # Interrupted while the operation was running: stop this run's container only.
        try {
            Write-RunLog 'ERROR' 'interrupted while the operation was running'
            Stop-OwnRun
        } catch {
            [Console]::Error.WriteLine("cleanup after interruption failed: $($_.Exception.Message)")
        }
        $Status.outcome = 'FAILED'
        $Status.reason = 'interrupted while the operation was running'
        $exitCode = $ExitWrapperError
    }
    $endedAt = [DateTime]::UtcNow
    $Status.exitCode = $exitCode
    $Status.exitClass = $ExitClasses[$exitCode]
    if ($null -eq $Status.exitClass) { $Status.exitClass = 'UNKNOWN' }
    $Status.endedAtUtc = Format-Utc $endedAt
    $Status.durationSeconds = [Math]::Round(($endedAt - $StartedAt).TotalSeconds, 3)
    if ($null -ne $script:LogFile) {
        try {
            if (-not $PreflightOnly) { Write-StatusFile }
            Remove-ExpiredLogs
            Write-RunLog 'INFO' ("run {0} finished: outcome {1}, exit {2} ({3}), {4} s" -f `
                $RunId, $Status.outcome, $exitCode, $Status.exitClass, $Status.durationSeconds)
        } catch {
            [Console]::Error.WriteLine("could not record the run status: $($_.Exception.Message)")
            if ($exitCode -eq 0) { $exitCode = $ExitWrapperError }
        }
    }
}
exit $exitCode
