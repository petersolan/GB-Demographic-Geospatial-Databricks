# Run the unit tests in the Linux test container (Spark on Windows needs Hadoop winutils).
# Usage:  .\scripts\run_tests.ps1            (add -Build to rebuild the image first)
param([switch]$Build)
$repo = Split-Path -Parent $PSScriptRoot
if ($Build -or -not (docker image inspect gbdemo-test 2>$null)) {
    docker build -f "$repo\Dockerfile.test" -t gbdemo-test $repo
}
docker run --rm -v "${repo}:/repo" gbdemo-test pytest -q @args
