# Advanced Techniques to Supercharge PowerShell PowerCLI Automation (VMug 2023)   

## Play with me LIVE DEMO: 1 to 100


```powershell   

Measure-Command { 1..1000 | ForEach-Object -ThrottleLimit 500 -Parallel { sleep 1 ; $_ }  }

```

Try Tweeking the -ThrottleLimit integer and Sleep integer to see how it affects the total time



## Count Virtual Machines *Fast*
```powershell   
Get-View -ViewType VirtualMachine -Property Name  | Measure-Object | % { $_.Count }
```


## Count  ESXi VMhosts *Fast*

```powershell   
Get-View -ViewType HostSystem -Property Name  | Measure-Object | % { $_.Count }
```

## -Filters 
HostSystem -Filter for VMhost names for *faster* retreival

```powershell   
Get-View -ViewType HostSystem -Property Name  -Filter @{Name="myhostname" }  | Measure-Object | % { $_.Count }
```

VirtualMachine -Filter  Name for *faster* retreival

```powershell   
Get-View -ViewType VirtualMachine -Property Name  -Filter @{Name="MyVmName" }  | Measure-Object | % { $_.Count }
```

Do the previous with but for an EXACT Name match
```powershell
$myVm = "myvmName"
Get-View -ViewType VirtualMachine -Property Name  -Filter @{Name="$myVm" }  | Where-Object Name -eq $myVm | Measure-Object | % { $_.Count }
```



## Count all Snapshots
```powershell   
Get-View -ViewType VirtualMachine -Property Snapshot |
  Where-Object { $_.Snapshot.RootSnapshotList.count } |
    % { $_.Snapshot.CurrentSnapshot ; $_.CurrentSnapshotList  }  | Measure-Object
```




