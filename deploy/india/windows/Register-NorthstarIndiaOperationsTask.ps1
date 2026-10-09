<#
.SYNOPSIS
    Registers the Northstar India operations task for the current user, DISABLED.

.DESCRIPTION
    Renders northstar-india-operations.task.xml with absolute paths and the
    current Windows user, then registers it with Task Scheduler. The task:

    - runs Invoke-NorthstarIndiaOperations.ps1 from this directory, hourly;
    - runs only while this user is signed in -- the Docker Desktop session --
      with an interactive token, so no password is stored, and least privilege;
    - never starts a second instance while one is running, and starts a missed
      run as soon as possible;
    - is registered DISABLED. This script never enables it, never overwrites an
      existing task and never reads .env.

    Run it as the Windows user that runs Docker Desktop, from this directory of
    the India checkout. Use -RenderPath to write the rendered XML for review
    without registering anything.

.PARAMETER TaskName
    The Task Scheduler task name, in the root task folder.

.PARAMETER LogDirectory
    Optional log directory passed to the wrapper. When omitted the wrapper uses
    %LOCALAPPDATA%\Northstar\india-operations of this user.

.PARAMETER RenderPath
    Write the rendered task XML to this file and register nothing.
#>
[CmdletBinding()]
param(
    [string] $TaskName = 'Northstar India Operations',
    [string] $LogDirectory,
    [string] $RenderPath
)

Set-StrictMode -Version 2.0
$ErrorActionPreference = 'Stop'

function ConvertTo-XmlText([string] $Value) {
    return [System.Security.SecurityElement]::Escape($Value)
}

function Get-FullPath([string] $Path) {
    $full = [System.IO.Path]::GetFullPath($Path)
    # A trailing backslash would escape the closing quote of a quoted argument.
    if ($full.Length -gt 3) { $full = $full.TrimEnd('\') }
    return $full
}

$wrapper = Get-FullPath (Join-Path $PSScriptRoot 'Invoke-NorthstarIndiaOperations.ps1')
$template = Get-FullPath (Join-Path $PSScriptRoot 'northstar-india-operations.task.xml')
$deployment = Get-FullPath (Join-Path $PSScriptRoot '..')
$powershell = Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
foreach ($required in @($wrapper, $template, $powershell, (Join-Path $deployment 'compose.yaml'))) {
    if (-not [System.IO.File]::Exists($required)) {
        throw "Required file not found: $required"
    }
}

$arguments = '-NoProfile -NonInteractive -ExecutionPolicy Bypass -File "{0}"' -f $wrapper
if ($LogDirectory) {
    $arguments += ' -LogDirectory "{0}"' -f (Get-FullPath $LogDirectory)
}
$user = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name

$xml = [System.IO.File]::ReadAllText($template)
$xml = $xml.Replace('{{USER_ID}}', (ConvertTo-XmlText $user))
$xml = $xml.Replace('{{POWERSHELL}}', (ConvertTo-XmlText $powershell))
$xml = $xml.Replace('{{ARGUMENTS}}', (ConvertTo-XmlText $arguments))
# Unquoted on purpose: Task Scheduler rejects a quoted working directory.
$xml = $xml.Replace('{{WORKING_DIRECTORY}}', (ConvertTo-XmlText $deployment))
if ($xml -match '\{\{[A-Z_]+\}\}') {
    throw 'The task template still holds an unrendered placeholder.'
}

$document = New-Object System.Xml.XmlDocument
$document.LoadXml($xml)
$namespaces = New-Object System.Xml.XmlNamespaceManager($document.NameTable)
$namespaces.AddNamespace('t', 'http://schemas.microsoft.com/windows/2004/02/mit/task')
$enabled = $document.SelectSingleNode('/t:Task/t:Settings/t:Enabled', $namespaces)
if ($null -eq $enabled -or $enabled.InnerText -ne 'false') {
    throw 'The task definition must be registered disabled.'
}

if ($RenderPath) {
    $target = [System.IO.Path]::GetFullPath($RenderPath)
    [System.IO.File]::WriteAllText($target, $xml, (New-Object System.Text.UTF8Encoding($false)))
    Write-Output "Rendered (not registered): $target"
    return
}

if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
    throw (
        ("A task named '{0}' already exists and was left unchanged. " -f $TaskName) +
        'Unregister it first (runbook section 18) or choose another -TaskName.'
    )
}
$null = Register-ScheduledTask -TaskName $TaskName -Xml $xml
$registered = Get-ScheduledTask -TaskName $TaskName
if ([string] $registered.State -ne 'Disabled') {
    $null = Disable-ScheduledTask -TaskName $TaskName
    throw "Task '$TaskName' was not registered disabled; it has been disabled. Check its definition."
}
Write-Output "Registered '$TaskName' for $user, DISABLED."
Write-Output "Wrapper: $wrapper"
Write-Output 'Enable it only after a successful manual run (runbook section 18).'
