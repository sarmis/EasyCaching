param (
  [Parameter(Mandatory=$true)][string]$key
)

$folder = ".\output\packages"

$projects = @(
    "EasyCaching.Core",        
    "EasyCaching.Bus.RabbitMQ",    
    "EasyCaching.Bus.Redis",    
    "EasyCaching.CSRedis",    
    "EasyCaching.Redis",    
    "EasyCaching.HybridCache",    
    "EasyCaching.InMemory",
    "EasyCaching.Serialization.Json"
);

# Clear folder before publishing PackagesFolder
remove-item $folder -recurse -force -confirm:$false

# Try to pack ALL projects to PackagesFolder (apps will fail without affecting the rest)
dotnet pack -o $folder -c Release .\EasyCaching.sln

foreach ($project in $projects)
{    
    Write-Host $project;
    $nuget = (Get-ChildItem "$folder\$project.*.nupkg")[0].name;
    dotnet nuget push "$folder\$nuget" -s https://nuget.kaizengaming.eu/ -k $key --skip-duplicate
}