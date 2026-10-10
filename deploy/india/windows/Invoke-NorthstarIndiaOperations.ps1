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
    0-7  Northstar's own exit code, passed through unchanged. 7 is an expiry
         exception: the operated contract is out of sessions or past its
         expiration date while a position or pending order remains in it.
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
    EXPIRY_EXCEPTION
               exit 7 and Northstar printed "STATUS: EXPIRY EXCEPTION". It is
               never WAITING or a rollover: follow the Expiry Exception
               Operator Procedure.
    FAILED     any other non-zero exit, or exit 0 without a recognised status
               line.

.PARAMETER DeploymentDirectory
    The India deployment directory holding compose.yaml and .env. Defaults to
    the parent of this script's directory. The default is resolved in the
    script body: Windows PowerShell 5.1 leaves $PSScriptRoot empty while an
    advanced script's parameter defaults are evaluated.

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

.PARAMETER ProjectName
    The Compose project. Production omits it: the default, northstar-india, is
    the production project and its execution path is unchanged.

    Any other value is an isolated acceptance project and must be named
    northstar-india-acceptance-<id>. Acceptance runs fail closed (exit 11),
    before any log or status file is written or Docker is called, unless
    -DeploymentDirectory and -LogDirectory are both given explicitly as local
    drive paths (never UNC or device paths) that, resolved through junctions,
    symbolic links and subst drives, overlap neither production directory, and
    the log directory holds no last-run.json from another project. After
    `docker compose config`, the rendered Compose model is checked against an
    allowlist: only session-named local volumes without driver options, only
    internal session bridge networks, session volume mounts only, the
    northstar-api:india image with pull_policy never, NORTHSTAR_* variables
    without credentials, and no other setting in force.
#>
[CmdletBinding()]
param(
    [string] $DeploymentDirectory,
    [string] $LogDirectory = (Join-Path $env:LOCALAPPDATA 'Northstar\india-operations'),
    [ValidateRange(1, 86400)] [int] $TimeoutSeconds = 2700,
    [ValidateRange(1, 3600)] [int] $PreflightTimeoutSeconds = 60,
    [ValidateRange(0, 36500)] [int] $LogRetentionDays = 90,
    [string] $DockerExecutable = 'docker',
    [switch] $PreflightOnly,
    [string] $ProjectName = 'northstar-india'
)

Set-StrictMode -Version 2.0
$ErrorActionPreference = 'Stop'

$BoundAtStart = $PSBoundParameters
if (-not $BoundAtStart.ContainsKey('DeploymentDirectory')) {
    # Resolved here, not as a parameter default: see .PARAMETER DeploymentDirectory.
    $DeploymentDirectory = Join-Path $PSScriptRoot '..'
}
$ProductionProjectName = 'northstar-india'
# Only these names may run outside production; anything else is refused.
$AcceptanceProjectPattern = '^northstar-india-acceptance-[a-z0-9][a-z0-9-]{3,39}$'
# Named explicitly for a clear message; the session-prefix rule already refuses
# every other stack's volumes too.
$ProductionVolumeNames = @(
    'northstar-india-data', 'northstar-india-caddy-data', 'northstar-india-caddy-config'
)
$IsAcceptance = -not ($ProjectName -ceq $ProductionProjectName)
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
    6 = 'PROVIDER'; 7 = 'EXPIRY_EXCEPTION'; 10 = 'DOCKER_UNAVAILABLE'; 11 = 'DEPLOYMENT_INVALID'
    12 = 'TIMEOUT'; 13 = 'WRAPPER_ERROR'
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
    projectName         = $ProjectName
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

function Test-PathOverlap([string] $First, [string] $Second) {
    $one = $First.TrimEnd('\') + '\'
    $two = $Second.TrimEnd('\') + '\'
    return $one.StartsWith($two, [StringComparison]::OrdinalIgnoreCase) -or
        $two.StartsWith($one, [StringComparison]::OrdinalIgnoreCase)
}

function Resolve-AcceptancePath([string] $Path) {
    # Acceptance only. The final path of the deepest existing ancestor -- through
    # junctions, symbolic links, subst drives and 8.3 names -- plus the remainder
    # that does not exist yet. Compiled on first use; production never loads it.
    if (-not ('NorthstarAcceptancePath' -as [type])) {
        Add-Type -TypeDefinition @'
using System;
using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Text;
using Microsoft.Win32.SafeHandles;

public static class NorthstarAcceptancePath
{
    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern SafeFileHandle CreateFileW(
        string name, uint access, uint share, IntPtr security, uint disposition, uint flags, IntPtr template);

    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern uint GetFinalPathNameByHandleW(
        SafeFileHandle handle, StringBuilder path, uint length, uint flags);

    // No access rights, shared, existing only, directories allowed (backup semantics).
    public static string Resolve(string path)
    {
        using (SafeFileHandle handle = CreateFileW(path, 0, 7, IntPtr.Zero, 3, 0x02000000, IntPtr.Zero))
        {
            if (handle.IsInvalid) { throw new Win32Exception(Marshal.GetLastWin32Error()); }
            StringBuilder buffer = new StringBuilder(1024);
            uint length = GetFinalPathNameByHandleW(handle, buffer, (uint) buffer.Capacity, 0);
            if (length >= buffer.Capacity)
            {
                buffer = new StringBuilder((int) length + 1);
                length = GetFinalPathNameByHandleW(handle, buffer, (uint) buffer.Capacity, 0);
            }
            if (length == 0 || length >= buffer.Capacity) { throw new Win32Exception(Marshal.GetLastWin32Error()); }
            return buffer.ToString();
        }
    }
}
'@
    }
    $existing = [System.IO.Path]::GetFullPath($Path)
    $remainder = New-Object System.Collections.Generic.List[string]
    # Attributes are -1 only when nothing is there; a dangling link is not skipped
    # over, it fails to resolve below.
    while ([int] [System.IO.DirectoryInfo]::new($existing).Attributes -eq -1) {
        $parent = [System.IO.Path]::GetDirectoryName($existing)
        if (-not $parent) { throw "no part of $Path exists" }
        $remainder.Insert(0, [System.IO.Path]::GetFileName($existing))
        $existing = $parent
    }
    $final = [NorthstarAcceptancePath]::Resolve($existing)
    if ($final.StartsWith('\\?\UNC\', [StringComparison]::OrdinalIgnoreCase)) {
        throw "$Path resolves to a network path"
    }
    if ($final.StartsWith('\\?\')) { $final = $final.Substring(4) }
    if ($final -notmatch '^[A-Za-z]:\\') { throw "$Path resolves to $final, which is not a drive path" }
    foreach ($part in $remainder) { $final = [System.IO.Path]::Combine($final, $part) }
    return $final
}

function Get-AcceptancePathRefusal([string] $Label, [string] $Raw) {
    # The text as given: a local drive path, nothing that needs interpreting.
    if ($Raw -match '^[\\/]{2}') {
        return "acceptance $Label '$Raw' is a UNC or device path; only local drive paths are allowed"
    }
    if ($Raw -notmatch '^[A-Za-z]:[\\/]' -or $Raw.IndexOf(':', 2) -ge 0) {
        return "acceptance $Label '$Raw' is not an absolute local drive path"
    }
    return $null
}

function Get-AcceptanceRefusal {
    # Acceptance only, before anything is written or Docker is called. Paths are
    # compared as resolved, so an alias of a production directory is refused.
    if ($ProjectName -cnotmatch $AcceptanceProjectPattern) {
        return ("project name '{0}' is neither the production default nor an acceptance " -f $ProjectName) +
            'name (northstar-india-acceptance-<id>)'
    }
    foreach ($name in @('DeploymentDirectory', 'LogDirectory')) {
        if (-not $BoundAtStart.ContainsKey($name)) {
            return "an acceptance run must pass -$name explicitly; its default is production's"
        }
    }
    $given = [ordered]@{ 'deployment directory' = $DeploymentDirectory; 'log directory' = $LogDirectory }
    foreach ($label in $given.Keys) {
        $refusal = Get-AcceptancePathRefusal $label $given[$label]
        if ($refusal) { return $refusal }
    }
    $resolved = [ordered]@{}
    foreach ($label in $given.Keys) {
        try {
            $resolved[$label] = Resolve-AcceptancePath $given[$label]
        } catch {
            return "acceptance $label '$($given[$label])' could not be resolved safely: $($_.Exception.Message)"
        }
        if ([System.IO.DriveInfo]::new($resolved[$label].Substring(0, 1)).DriveType -ne 'Fixed') {
            return "acceptance $label $($resolved[$label]) is not on a local fixed drive"
        }
    }
    $production = @(
        @('deployment directory', (Resolve-AcceptancePath (Join-Path $PSScriptRoot '..'))),
        @('log directory', (Resolve-AcceptancePath (Join-Path $env:LOCALAPPDATA 'Northstar\india-operations'))),
        @('log directory', (Resolve-AcceptancePath (
            Join-Path ([Environment]::GetFolderPath('LocalApplicationData')) 'Northstar\india-operations')))
    )
    foreach ($label in $resolved.Keys) {
        foreach ($entry in $production) {
            if (Test-PathOverlap $resolved[$label] $entry[1]) {
                return "acceptance $label $($resolved[$label]) overlaps the production $($entry[0]) $($entry[1])"
            }
        }
    }
    # Whatever the path, never replace a status file this session did not write.
    $existingStatus = [System.IO.Path]::Combine($resolved['log directory'], 'last-run.json')
    if ([System.IO.File]::Exists($existingStatus)) {
        $owner = $null
        try {
            $owner = Get-Field ([System.IO.File]::ReadAllText($existingStatus) | ConvertFrom-Json) 'projectName'
        } catch {
            $owner = $null
        }
        if (-not ($owner -is [string]) -or $owner -cne $ProjectName) {
            return ("acceptance log directory {0} already holds a last-run.json that is not from {1} " -f
                $resolved['log directory'], $ProjectName) + "(production's, another session's or unreadable)"
        }
    }
    return $null
}

function Get-Field($Object, [string] $Name) {
    if ($null -eq $Object) { return $null }
    $property = $Object.PSObject.Properties[$Name]
    if ($null -eq $property) { return $null }
    return $property.Value
}

function Get-Fields($Object) {
    if ($null -eq $Object) { return @() }
    return @($Object.PSObject.Properties)
}

function Test-Set($Value) {
    # In force: Compose renders a setting that is not as absent, null, false or empty.
    if ($null -eq $Value) { return $false }
    if ($Value -is [bool]) { return $Value }
    if ($Value -is [string]) { return $Value.Length -gt 0 }
    if ($Value -is [System.Management.Automation.PSCustomObject]) {
        return @($Value.PSObject.Properties).Count -gt 0
    }
    if ($Value -is [System.Array]) { return $Value.Count -gt 0 }
    return $true
}

function Get-SetFields($Object) {
    return @(Get-Fields $Object | Where-Object { Test-Set $_.Value })
}

function Get-AcceptanceModelProblems([string] $Json) {
    # An allowlist: every setting in force must be one acceptance needs, so an
    # unknown or newer Compose key is refused, not overlooked. A session-prefixed
    # name alone proves nothing (a local volume can bind any path through its
    # driver options), so the backing configuration is checked too. Names only
    # in messages: environment values are never repeated.
    $topLevelKeys = @('name', 'services', 'volumes', 'networks')
    $serviceKeys = @('image', 'pull_policy', 'command', 'entrypoint', 'environment', 'volumes', 'networks')
    $mountKeys = @('type', 'source', 'target', 'read_only')
    $credentialPattern = 'UPSTOX|TOKEN|SECRET|PASSWORD|PASSWD|CREDENTIAL|AUTH|API_?KEY|ACCESS_?KEY'
    $problems = New-Object System.Collections.Generic.List[string]
    try {
        $model = $Json | ConvertFrom-Json
    } catch {
        return @('the rendered Compose model is not JSON')
    }
    if ((Get-Field $model 'name') -cne $ProjectName) {
        $problems.Add("the rendered project is not $ProjectName")
    }
    foreach ($field in Get-SetFields $model) {
        if ($topLevelKeys -cnotcontains $field.Name) { $problems.Add("top-level $($field.Name) are not allowed") }
    }
    $prefix = "$ProjectName-"
    $volumeKeys = @{}
    foreach ($volume in Get-Fields (Get-Field $model 'volumes')) {
        $volumeKeys[$volume.Name] = $true
        $name = [string] (Get-Field $volume.Value 'name')
        if (-not $name.StartsWith($prefix, [StringComparison]::Ordinal)) {
            $problems.Add("volume '$($volume.Name)' is named '$name', not $prefix<name>")
        }
        if ($ProductionVolumeNames -contains $name) { $problems.Add("volume '$name' is a production volume") }
        foreach ($field in Get-SetFields $volume.Value) {
            switch -CaseSensitive ($field.Name) {
                'name' { }
                'driver' {
                    if ($field.Value -cne 'local') { $problems.Add("volume '$name' uses driver '$($field.Value)'; only local is allowed") }
                }
                'external' { $problems.Add("volume '$name' is external") }
                'driver_opts' { $problems.Add("volume '$name' sets driver_opts, which can alias other storage under a session name") }
                default { $problems.Add("volume '$name' sets $($field.Name), which acceptance does not allow") }
            }
        }
    }
    $networkKeys = @{}
    foreach ($network in Get-Fields (Get-Field $model 'networks')) {
        $networkKeys[$network.Name] = $true
        $name = [string] (Get-Field $network.Value 'name')
        if (-not $name.StartsWith($prefix, [StringComparison]::Ordinal)) {
            $problems.Add("network '$($network.Name)' is named '$name', not $prefix<name>")
        }
        if (-not (Test-Set (Get-Field $network.Value 'internal'))) {
            $problems.Add("network '$name' is not internal")
        }
        foreach ($field in Get-SetFields $network.Value) {
            switch -CaseSensitive ($field.Name) {
                'name' { }
                'internal' { }
                'driver' {
                    if ($field.Value -cne 'bridge') { $problems.Add("network '$name' uses driver '$($field.Value)'; only bridge is allowed") }
                }
                'external' { $problems.Add("network '$name' is external") }
                'driver_opts' { $problems.Add("network '$name' sets driver_opts, which acceptance does not allow") }
                default { $problems.Add("network '$name' sets $($field.Name), which acceptance does not allow") }
            }
        }
    }
    $services = @(Get-Fields (Get-Field $model 'services'))
    if (-not @($services | Where-Object { $_.Name -ceq $Service })) {
        $problems.Add("service $Service is not defined")
    }
    foreach ($entry in $services) {
        $definition = $entry.Value
        $label = "service '$($entry.Name)'"
        foreach ($field in Get-SetFields $definition) {
            if ($serviceKeys -cnotcontains $field.Name) {
                $problems.Add("$label sets $($field.Name), which acceptance does not allow")
            }
        }
        if ((Get-Field $definition 'image') -cne $Image) { $problems.Add("$label does not use $Image") }
        if ((Get-Field $definition 'pull_policy') -cne 'never') { $problems.Add("$label is not pull_policy never") }
        foreach ($mount in @(Get-Field $definition 'volumes')) {
            if ($null -eq $mount) { continue }
            $type = [string] (Get-Field $mount 'type')
            $source = [string] (Get-Field $mount 'source')
            $target = [string] (Get-Field $mount 'target')
            if ($type -cne 'volume' -or -not $volumeKeys.ContainsKey($source)) {
                $problems.Add("$label mounts $type '$source' at '$target'; only session volumes are allowed")
            }
            if ($source -match 'docker\.sock' -or $target -match 'docker\.sock') {
                $problems.Add("$label mounts the Docker socket")
            }
            foreach ($field in Get-SetFields $mount) {
                if ($mountKeys -cnotcontains $field.Name) {
                    $problems.Add("$label mount at '$target' sets $($field.Name), which acceptance does not allow")
                }
            }
        }
        foreach ($variable in Get-Fields (Get-Field $definition 'environment')) {
            if ($variable.Name -match $credentialPattern) {
                $problems.Add("$label passes provider credential variable $($variable.Name)")
            } elseif ($variable.Name -cnotmatch '^NORTHSTAR_[A-Z0-9_]+$') {
                $problems.Add("$label passes variable $($variable.Name), which acceptance does not allow")
            }
        }
        $attached = @(Get-Fields (Get-Field $definition 'networks'))
        if ($attached.Count -eq 0) { $problems.Add("$label is not attached to a session network") }
        foreach ($network in $attached) {
            if (-not $networkKeys.ContainsKey($network.Name)) {
                $problems.Add("$label joins network '$($network.Name)' that is not session-owned")
            }
            if (Test-Set $network.Value) {
                $problems.Add("$label joins network '$($network.Name)' with attachment settings")
            }
        }
    }
    return $problems.ToArray()
}

function Invoke-Operation {
    $deployment = [System.IO.Path]::GetFullPath($DeploymentDirectory)
    $composeFile = Join-Path $deployment 'compose.yaml'
    $envFile = Join-Path $deployment '.env'
    $Status.deploymentDirectory = $deployment

    Write-RunLog 'INFO' "run $RunId started by $env:USERDOMAIN\$env:USERNAME on $env:COMPUTERNAME"
    Write-RunLog 'INFO' "deployment directory: $deployment"
    if ($IsAcceptance) {
        Write-RunLog 'INFO' "compose project: $ProjectName (isolated acceptance; production project, paths and volumes are refused)"
    } else {
        Write-RunLog 'INFO' "compose project: $ProjectName"
    }
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
    if ($IsAcceptance) {
        # Acceptance only: the production path never runs this inspection.
        $rendered = Invoke-Docker ($composeBase + @('config', '--format', 'json')) $PreflightTimeoutSeconds
        if ($rendered.TimedOut -or $rendered.ExitCode -ne 0) {
            return Set-Failure $ExitDeploymentInvalid 'acceptance: the rendered Compose model could not be inspected; refusing to run'
        }
        $problems = @(Get-AcceptanceModelProblems $rendered.Stdout)
        if ($problems.Count -gt 0) {
            return Set-Failure $ExitDeploymentInvalid ('acceptance configuration refused: ' + ($problems -join '; '))
        }
        Write-RunLog 'INFO' 'acceptance isolation verified: session-owned volumes and internal networks only'
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
        Where-Object { $_ -match '^[A-Z_]+ ERROR: ' } |
        Select-Object -First 1
    $Status.statusLine = $statusLine
    if ($code -eq 7) {
        # Never reported as WAITING or as a rollover, whatever else was printed.
        $Status.outcome = 'EXPIRY_EXCEPTION'
        if ($errorLine) { $Status.reason = $errorLine } else { $Status.reason = 'expiry exception (exit 7)' }
    } elseif ($code -ne 0) {
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

if ($IsAcceptance) {
    # Fail closed before any file is written or Docker is called. The production
    # default never takes this branch.
    $refusal = $null
    try {
        $refusal = Get-AcceptanceRefusal
    } catch {
        $refusal = "the acceptance safety check could not complete: $($_.Exception.Message)"
    }
    if ($refusal) {
        [Console]::Error.WriteLine("REFUSED (exit $ExitDeploymentInvalid): $refusal. No log or status file was written and Docker was not called.")
        exit $ExitDeploymentInvalid
    }
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
