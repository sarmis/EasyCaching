param (
  [Parameter(Mandatory=$true)][string]$key
)

$folder = ".\output\packages"



# Clear folder before publishing PackagesFolder
remove-item $folder -recurse -force -confirm:$false

# Try to pack ALL projects to PackagesFolder (apps will fail without affecting the rest)
dotnet pack -o $folder -c Release bus/EasyCaching.Bus.RabbitMQStream/EasyCaching.Bus.RabbitMQStream.csproj
dotnet pack -o $folder -c Release bus/EasyCaching.Bus.RabbitMQ/EasyCaching.Bus.RabbitMQ.csproj

$nuget = (Get-ChildItem "$folder\EasyCaching.Bus.RabbitMQStream.*.nupkg")[0].name;
dotnet nuget push "$folder\$nuget" -s https://nuget.kaizengaming.eu/ -k $key --skip-duplicate

$nuget = (Get-ChildItem "$folder\EasyCaching.Bus.RabbitMQ.*.nupkg")[0].name;
dotnet nuget push "$folder\$nuget" -s https://nuget.kaizengaming.eu/ -k $key --skip-duplicate
