# Check docker daemon
$dd = [io.path]::Combine($env:ProgramFiles, "Docker", "Docker", "Docker for Windows.exe")
if (!(Test-Path $dd)) {
    Write-Error "No Docker Daemon at" $dd
    exit
}

# Start the daemon (noop if already started)
Write-Output "Starting docker daemon."
Write-Output "If this is the first time, please wait until it's fully started"
& $dd

$localmnt = Convert-Path .
$dockermnt = "/home/azureml/cntk"
$image = "cntkhdfs.azurecr.io/dev/hdfs"

Write-Output ("[Executing]: docker run -it --rm -v " + $localmnt + ":" + $dockermnt + " " + $image)
docker run -it --rm -v ${localmnt}:${dockermnt} $image