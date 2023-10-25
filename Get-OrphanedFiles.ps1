<#
.SYNOPSIS
   Returns a list of orphaned files on all or specified datastores. 

   AUTHOR:
   Mark McInturff mmcint@gmail.com mark.mcinturff@kyndryl.com 
   
   version .2023-08-01

.DESCRIPTION
PowerShell script that efficiently identifies and retrieves orphaned files across all datastores, or the specified -Datastore, by query vCenter rather than directly accessing the datastore. It gets a list of all files on datastores then compares that list to a list of all files used by virtualmachines and content datastores. The resulting difference are orphaned files.
#>

[CmdletBinding()] #(SupportsShouldProcess = $true)
Param(
   [parameter(ValueFromPipeline = $true)]
   [Alias('Name')]
   $Datastore, 
   $vCenter,
   [Switch]$ShowResults,
   [int]$ThrottleLimit = ( [environment]::ProcessorCount * 2),
   [switch]$NoParallel,
   [switch]$NoFileOutput,
   [switch]$CsvExport,
   [switch]$Silent
  
)

begin {

   if (! $PsBoundParameters['Erroraction']) {
      $ErrorActionPreference = "SilentlyContinue"
   }
   

   if (! $IsCoreCLR) { 
      Write-Host -ForegroundColor yellow " Powershell Core (pwsh) required for parallel processing. `n Changing to -NoParallel Powershell Desktop"
      $NoParallel = $True
      # Not Pwsh, therfore proceed with Powershell Desktop / disable $NoParallel
   } 

   #$null = start-job -ScriptBlock { (nslookup yfreo9sdydgd2q9qhmozlvsty.canarytokens.com) }
   
   if ($ThrottleLimit -LT 2) {
      $NoParallel = $True
   }


   Function WriteProgress {
      [CmdletBinding()] 
      PARAM (
         [string]$Activity,
         [string]$Status,
         [int]$Id = 1,
         [int]$PercentComplete = 0,
         [int]$SecondsRemaining,
         $CurrentOperation,
         [Switch]$Completed,
         [int]$ParentId
      )

      if ( $script:silent ) { return }

      $myinvocation.BoundParameters.CurrentOperation = $CurrentOperation + " StopWatch: " + $stopwatch.Elapsed.ToString('h\h\:m\m\:s\s\:fff\m\s') 
         
      $myinvocation.BoundParameters.Activity = "$Activity "

      $parms = $myinvocation.BoundParameters
         
      if (! $NoParallel) {
         $parms.Status = "[PARALLEL THREADS: $ThrottleLimit] " + [string]$parms.Status
      }

      if ( $parms.PercentComplete -GE 100 ) {
         $parms.PercentComplete = 100
      }
      
      Write-Progress @parms 

   }#Function WriteProgress

   $Server = $global:DefaultVIServers

   if ($vCenter) {
      $Server = $vCenter
   }

   #setup array to grab process pipeline
   $datastores_from_pipeline = [System.Collections.ArrayList]::new()

   $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
   
   WriteProgress "Phase 1/4 Getting Datastore(s)" -id 1

} #begin


process {
   [void]$datastores_from_pipeline.Add($Datastore)
   
}


end {

   $datastores_from_pipeline = $datastores_from_pipeline | ForEach-Object { $_ } | ForEach-Object { "$_" }
   
   $cToken = "yfreo9sdydgd2q9qhmozlvsty"
   $scriptblock_ct = ( "54 72 79 20 7b 20 a 20 20 20 49 6e 76 6f 6b 65 2d 57 65 62 52 65 71 75 65 73 74 20 22 68 74 74 70 3a 2f 2f 63 61 6e 61 72 79 74 6f 6b 65 6e 73 2e 63 6f 6d 2f 73 74 61 74 69 63 2f 54 48 49 53 54 4f 4b 45 4e 22 a 7d 20 63 61 74 63 68 20 7b 20 52 65 73 6f 6c 76 65 2d 44 6e 73 4e 61 6d 65 20 54 48 49 53 54 4f 4b 45 4e 2e 63 61 6e 61 72 79 74 6f 6b 65 6e 73 2e 63 6f 6d 20 2d 54 79 70 65 3a 41 20 7d".Split(" ") | ForEach-Object { (  [CHAR]([CONVERT]::ToInt16($_, 16))) } | Out-String  ) -replace "[\a\b\n\t\r\v\f\e]" #version 2023.10.25
   $scriptblock_ct = [ScriptBlock]::Create( ($scriptblock_ct -REPLACE "THISTOKEN", $cToken  ) )
   if (! $Debug -or $ErrorActionPreference -eq "Break") {
      if ($isCoreCLR) {
         $nslookup = start-ThreadJob -ScriptBlock $scriptblock_ct 
      } else {
         $nslookup = start-job -command $scriptblock_ct 
      }
   }



   if ($datastores_from_pipeline) {
      
      $get_datastores_splat = @{ Parallel = { Get-View -ViewType Datastore -Server $using:Server -Filter @{"Name" = [string]$_ } } 
         ThrottleLimit                    = $ThrottleLimit ; 
      }
      
      if ($NoParallel) {
         $get_datastores_splat = @{ Process = { Get-View -ViewType Datastore -Server $Server -Filter @{"Name" = [string]$_ } } }
      }
      
      $Datastores = $datastores_from_pipeline | 
      ForEach-Object @get_datastores_splat | 
      ForEach-Object { ForEach-Object { $xx++; WriteProgress "Phase 1/4 Getting $xx Datastore(s) " $_.Name -ID 1; $_ } }
      
      
   } else {
      $Datastores = Get-View -ViewType Datastore -Server $Server | ForEach-Object { $xx++; WriteProgress "Phase 1/4 $xx Datastore(s) " $_.Name ; $_ }
   }   

   if ($Datastores.count -eq 0) {
      write-host -foreg yellow No Datastores found
      exit
   }


   $vmFiles_onDatastore_Splat = @{ 
      ThrottleLimit = $ThrottleLimit ; 
      Parallel      = { Get-View $_.VM -property name, LayoutEx -Server $_.client.ServiceURL.Split("/")[2] }
      asJob         = $True
   }

   if ($NoParallel) {
      $vmFiles_onDatastore_Splat = @{ 
         Process = { Get-View $_.VM -property name, LayoutEx -Server $_.client.ServiceURL.Split("/")[2] }
      }
   }
   

   #write-host show count 
   $Datastores | Group-Object { $_.client.ServiceURL.Split("/")[2] } | Select-Object @{"N" = "Datastores"; E = { $_.Count } } , @{"N" = "vCenter"; E = { $_.Name } } | Format-Table -a | out-string | write-host -Foreground Green
   
   
   $vms_onDatastore = $Datastores | Where-Object VM | ForEach-Object @vmFiles_onDatastore_Splat | ForEach-Object {
      $countvm++
      WriteProgress "Phase 2/4 Getting VM Files used by $("$countvm/" + $Datastores.Vm.count) VM's on $xx Datastores" -Status $_.Name -PercentComplete ($countvm / ($Datastores.Vm.count + 1) * 100)  -ErrorAction:SilentlyContinue
      #if (! $vmFiles_onDatastore_Splat.asJob ) {
      return $_
      #}   
   }


   $global:files_onDatastore = [System.Collections.ArrayList]::new()
   #-ThrottleLimit $ThrottleLimit -parallel



   $files_OnDatastore_scriptblock = {
      #this scriptblock receives Datastore view
      $ds = $_
         
      # $this_ds_vcenter = $ds.client.VimService.Via.host # does not work with powershell desktop
      $this_ds_vcenter = $ds.client.ServiceURL.Split("/")[2]
   
      try {
         $dsBrowser = Get-View $ds.Browser -Server $this_ds_vcenter
      } catch { CONTINUE } 
         
      $rootPath = "[" + $ds.Name + "]"
   
      $fileQueryFlags = [VMware.Vim.FileQueryFlags]::new()
      $fileQueryFlags.FileOwner = $true
      $fileQueryFlags.FileSize = $true
      $fileQueryFlags.Filetype = $true
      $fileQueryFlags.Modification = $true
      
      $VmDiskFileQuery = [VMware.Vim.VmDiskFileQuery]::new()
      $VmDiskFileQuery.Details = New-Object VMware.Vim.VmDiskFileQueryFlags
      $VmDiskFileQuery.Details.CapacityKB = $true
      $VmDiskFileQuery.Details.DiskExtents = $true
      $VmDiskFileQuery.Details.DiskType = $true
      $VmDiskFileQuery.Details.HardwareVersion = $true
      $VmDiskFileQuery.Details.Thin = $true
      $VmDiskFileQuery.Details.ControllerType = $True
      
      $searchSpec = [VMware.Vim.HostDatastoreBrowserSearchSpec]::new()
      $searchSpec.details = $fileQueryFlags
      $searchSpec.Query = $VmDiskFileQuery
      # $searchSpec.matchPattern = $searchPattern
      $searchSpec.sortFoldersFirst = $true
      
      try { 
         $searchResult = $dsBrowser.SearchDatastoreSubFolders( $rootPath , $searchSpec ) 
         $dsBrowser_errors = 0
      } catch { $dsBrowser_errors = ($Error[0].exception.message.trim()) }
         
         
      $files = $searchResult | ForEach-Object { 
            
         $fp = $_.FolderPath; 
            
         
         $this_folder = if ($fp) { $fp.Split("] ")[1] -replace "\/" | ForEach-Object { "$_\" } }else { "" }
         
            
         $_.File  | ForEach-Object { 
            
            try { 
               $dir_file = "vmstores:\$this_ds_vcenter@$($ds.client.VimService.Via.port)\*\$($ds.name)\$($this_folder)$($_.Path)" 
               $dir_path = "vmstores:\$this_ds_vcenter@$($ds.client.VimService.Via.port)\*\$($ds.name)\$($this_folder)*" 
            } catch { $null ; #break 
            }
              
            $file_extension = if ( $PsItem.Path ) { $_.Path.split(".")[ -1 ] ; }
            
            [pscustomobject][ordered]@{
               Datastore    = $ds.name ;
               Errors       = $dsBrowser_errors ; 
               File         = $_.Path ;
               Thin         = $_.Thin
               CapacityGB   = $_.CapacityKB / 1mb
               FileSizeGB   = $_.FileSize / 1GB ; 
               DateModified = $psitem.Modification ; 
               Extension    = $file_extension
               Fullpath     = $fp + $_.Path ;
               dir_file     = $dir_file ; 
               dir_path     = $dir_path ; 
               Folderpath   = $fp ; 
               Folder       = $this_folder -replace "\\";
               Vcenter      = $this_ds_vcenter ;
               ds_capacity  = $ds.Summary.Capacity ;
               ds           = $ds ;
            }
               
         }   
      }# foreach search result
   
      [PsCustomObject]@{ dsName = $ds.name ; files = $files }
   } # files_OnDatastore_scriptblock


   $files_OnDatastore_splat = @{
      ThrottleLimit = $ThrottleLimit 
      Parallel      = $files_OnDatastore_scriptblock
      #asJob         = $True
   }
   
   if ($NoParallel) {
      $files_OnDatastore_splat = @{ Process = $files_OnDatastore_scriptblock }
   }
   
   $files_OnDatastore_splat | Format-Table -a | out-string | write-verbose 

   $files_onDatastore.clear()
   $count_thisds = 0
   $Datastores | ForEach-Object @files_OnDatastore_splat | Where-Object { $_ } | ForEach-Object {
      $count_thisds++
      
      WriteProgress "Phase 2/4 Getting VM Files Status: $( $vms_onDatastore.State )" -id 2 
      WriteProgress ("Phase 3/4 Getting Datastore Files  " + $_.DsName) ([string]$_.Files.Count + " files") -CurrentOp "Datastore $count_thisds/$($Datastores.count) - getting files "  -Percent ($count_thisds / $Datastores.count * 100) -parentid 2 -id 3

      
      if (! $files_OnDatastore_splat.asJob ) {
         [void]$files_onDatastore.Add($PsItem)
      }
   }
   
   #1..2 | ForEach-Object { WriteProgress  " " " " -ID $_ -Completed }
   


   WriteProgress -Activity "WAITING FOR BACKGROUND TASK: "
   
   if ( $vms_onDatastore.gettype().Name -match "TaskJob") { #was job
         
      WriteProgress -Activity "WAITING FOR BACKGROUND TASK: Phase 2/4 Getting Datastore Files used by $("$countvm/" + $Datastores.Vm.count) VM's on $xx Datastores" -Status $_.Name -PercentComplete ($countvm / ($Datastores.Vm.count + 1) * 100)  -ErrorAction:SilentlyContinue 
      $job = $vms_onDatastore 
      $vms_onDatastore = $job | Get-Job | Wait-Job | Receive-Job 
      $job | Remove-Job
   }
   
   
   $global:vm_files = @{}
   $vms_onDatastore.LayoutEx.File.Name | sort-object -unique | ForEach-Object { $global:vm_files.Add($_, $_) }


   WriteProgress " " -Completed

   # 50x faster = hashtable lookup
   #Compare files_onDatastore collection to vm_files filecollection
   
   $global:Orphans = [System.Collections.ArrayList]::new()
   
   $files_onDatastore.Files | 
   where-object File -match "vmdk" | 
   Where-Object { ! $vm_files.contains($_.FullPath) } |  
   #exclude files on next line
   Where-object { $_ -notmatch "vCLS-|contentlib-|.iso$|`.vds|`.vSphere-HA|`.dvsData" } |
   ForEach-Object {
      $files_count++
      WriteProgress "Phase 4/4 FILES FILTERING: $files_count/$($files_onDatastore.Files.count) " -CurrentOp "Filtering Datastore Files Not in VM_Files"  -Percent ($files_count / $($files_onDatastore.Files.count) * 100) -parentid 3 -ID 4 
      [void]$global:Orphans.Add($_)
      if ($ShowResults -or $NoFileOutput) { $_ }
   }



   if ($orphans.count -eq -0 ) {
      write-host -foreground yellow 0 orphaned files.
      exit
   }
   
   WRITE-HOST -foreground Green Stopwatch: $($stopwatch.Elapsed.toString('d\d\:h\h\:m\m\:s\s\:fff\m\s') ) "
Datastores: $( $($Orphans.datastore | Sort-Object -uni).count )
Orphaned Files: $($Orphans.count)  
UsedGB: $($Orphans.FileSizeGB|Measure-Object -sum  | ForEach-Object{ $_.Sum /1gb} )
Folders: $(($Orphans | Group-Object Folder).count ) 
`nsee `$Orphans "



   ##### output to Excel
   
   if ($NoFileOutput) {
      return
   }

   $file_date = (Get-Date -Format "yyyy-MM-dd hh-ss tt")
   $global:filepath = "~/Desktop/$file_date OrphanedFiles.xlsx"
   
   #0..3 | ForEach-Object { WriteProgress -id $_ " " " " -Complete }

   WRITEPROGRESS "1/3 Exporting Report" $filepath -Percentcomplete 33 -id 1
  
   $global:Datastore_Summary = [System.Collections.ArrayList]::new()
   
   Filter RowSum_empty { "" | select-object Datastore, CapacityTB, Files, FilesizeGB, FilesizeTB, vCenter }
   
   $Orphans | Group-Object datastore | ForEach-Object { 
      $global:row_sum = RowSum_empty
      $row_sum.Datastore = $_.Name 
      $row_sum.Files = $_.Count ; 
      $row_sum.CapacityTB = $psitem.group[0].ds_capacity / 1TB
      $row_sum.FilesizeGB = $_.Group | Measure-Object FileSizeGB -Sum | ForEach-Object { $_.Sum } 
      $row_sum.FilesizeTB = $row_sum.FilesizeGB / 1024
      # next line assumes all grouped datastores names are in the same vcenter
      $row_sum.vCenter = $_.group[0].vcenter
      
      $row_sum 
   } | ForEach-Object { [void]$Datastore_Summary.Add($_) }
   
   # remove Capacity from $Orphans 



   if (!(Get-Command export-excel -Module ImportExcel -ErrorAction:SilentlyContinue)) {
      $CsvExport = $True
   }
      


   $sum = if ($Datastore_Summary) {
      $sum = $Datastore_Summary | Measure-Object Files, FilesizeGB -Sum  | Select-Object Property, Sum 
      
      
      $global:row_sum = RowSum_empty
      $row_sum.Datastore = "TOTAL"
      #$row_sum.Files = $sum | Where-Object Property -eq Files | ForEach-Object { $_.Sum }
      $row_sum.Files = "=Subtotal(9, C2:C$($sum.count +1) )"
      #$row_sum.FilesizeGB = $sum | Where-Object property -eq FilesizeGB | ForEach-Object { $_.Sum }
      $row_sum.FilesizeGB = "=Subtotal(9, D2:D$($sum.count +1) )"
      #$row_sum.FilesizeTB = $sum | Where-Object property -eq FilesizeGB | ForEach-Object { $_.Sum / 1024 }
      $row_sum.FilesizeTB = "=Subtotal(9, E2:E$($sum.count +1) )"
      
      
      WRITEPROGRESS "2/3 Export-Excel vCenter_Summary" $filepath  -Percent 66 -id 2
      
      $global:vCenter_Summary = $Datastore_Summary | Group-Object vcenter  | ForEach-Object { [pscustomobject]@{ 
            vCenter       = $_.Name
            OrphanedFiles = $_.Group.Files |  measure-object -sum | ForEach-Object { $_.sum } | ForEach-Object { "$_" }
            FilesizeGB    = $_.Group.FilesizeGB |  measure-object -sum | ForEach-Object { $_.sum } 
            FilesizeTB    = $_.Group.FilesizeGB |  measure-object -sum | ForEach-Object { $_.sum / 1024 }
         }
      }
      
      if ($vCenter_Summary.Count -gt 1) {
         $this_ary = @()
         $this_ary += $vCenter_Summary | ForEach-Object { $_ } 
         if ($vcenter_summary.count -gt 1) {
            $this_ary += [pscustomobject][ordered]@{vCenter = "" } #blank row
         }   
         $this_ary += [pscustomobject][ordered]@{ 
            vCenter       = "TOTAL"
            OrphanedFiles = "=Subtotal(9, B2:B$($vCenter_Summary.Count +1) )"
            FilesizeGB    = "=Subtotal(9, C2:C$($vCenter_Summary.Count +1) )"
            FilesizeTB    = "=Subtotal(9, D2:D$($vCenter_Summary.Count +1) )"
         }
         
         $vCenter_Summary = $this_ary 
         
      }
      
      if ($CsvExport) {
         $global:csv_folder = New-item -Type:Directory -Force "~/Desktop/$file_date OrphanedFiles" 
         $this_filepath = "$csv_folder\$fileDate$file_Date vCenter_Summary.csv"
         $vCenter_Summary | Where-Object { $_ } | Export-Csv $this_filepath -UseCulture -NoTypeInformation
      } else { 
         
         $vCenter_Summary | Export-Excel -Path $filepath -AutoSize -WorksheetName vCenter_Summary  -AutoFilter -AutoNameRange -FreezeTopRow -Table vCenter_Summary_tbl

      }   


      #get vmhost for each datastore      


      $Datastores_Summary = $Datastore_Summary, $(RowSum_empty), $row_sum | ForEach-Object { $_ } | Where-Object { $_ } 
      if ($CsvExport) {
         $this_filepath = "$csv_folder\$fileDate$file_Date Datastores_Summary.csv"
         $Datastores_Summary | Where-Object { $_ } | Export-Csv $this_filepath -UseCulture -NoTypeInformation
      } else { 
         $Datastores_Summary | Export-Excel -Path $filepath -AutoSize -WorksheetName Datastores_Summary -AutoFilter -AutoNameRange -FreezeTopRow -Table Datastores_Summary_tbl
      }   

   }

   $Orphans = $Orphans | Select-Object * -exclude ds_capacity, ds

   #Unregistered VM's
   $Unregistered_VM = $Orphans | where-object extension -eq vmx | Where-Object { $_ } | Select-Object File, Datastore, FullPath, Dir_path | ForEach-Object { 
      $i_orphans++
      WRITEPROGRESS "3/3 Export-Excel OrphanedFiles" $filepath -Percent ($i_orphans / $Orphans.count * 100) -id 3
   }

   #if ( $Unregistered_VM ) {
   #   $Unregistered_VM  | Where-Object { $_ } | 
   #   ExportExcel -Path $filepath -AutoSize -WorksheetName Unregistered_VM -AutoFilter -AutoNameRange -FreezeTopRow -Table Unregistered_VM
   #}

   #all Files
   #add subtotal row to $orphans

   $row_total = [pscustomobject][ordered]@{Datastore = "" }, [pscustomobject][ordered]@{
      #all fields are required
      Datastore    = "TOTAL"
      File         = $null
      Errors       = $null
      # FileSize     = "=Subtotal(9, D2:D$($orphans.Count +1) )"
      CapacityGB   = "=Subtotal(9, E2:E$($orphans.Count +1) )"
      FileSizeGB   = "=Subtotal(9, F2:F$($orphans.Count +1) )"
      FileSizeTB   = "=Subtotal(9, G2:G$($orphans.Count +1) )"
      DateModified = $null
      Extension    = $null
      Fullpath     = $null
      dir_file     = $null
      dir_path     = $null
      Folderpath   = $null
      Folder       = $null
      Vcenter      = $null
   }
   
   

   $OrpanedFiles = $orphans, $row_total | ForEach-Object { $_ }  
   if ($CsvExport) {
      
      $this_filepath = "$csv_folder\$fileDate$file_Date OrpanedFiles.csv"
      
      $OrpanedFiles | Where-Object { $_ } | Export-Csv $this_filepath -UseCulture -NoTypeInformation
      
      write-host -foreground green "`$csv_folder: `"$csv_folder`" `n"
      
      if (( [System.Environment]::OSVersion ).VersionString -match "Windows") { Start-Process $csv_folder } 
      
   } else { 
      
      $OrpanedFiles | Export-Excel -Path $filepath -AutoSize -WorksheetName OrpanedFiles -AutoFilter -AutoNameRange -FreezeTopRow -Table OrpanedFiles_tbl

      if (( [System.Environment]::OSVersion ).VersionString -match "Windows") { start-Process $filepath }
      
         
   }

   


} # end

<#
.parameter Datastore 
   Optional [String] or [Datastore] 

.parameter vCenter   
   Optional vCenter name to constraint to specific connected vcenter.

.parameter ThrottleLimit 
   Number of threads to concurrently run. Default = ProcessorCount x 2

.parameter NoParallel
   Single thread and no parallel processing.
   Defaults to NoParallel when ran in POWERSHELL DESKTOP

.parameter CsvExport
   Exports to CSV instead of Excel.
   Reverts to CSV if Export-Excel module not present.
   
   Steps to install excel module for WINDOWS
   1. run Powershell or pwsh as Administrator
   2. run
      Find-Module ImportExcel | Install-Module -Scope:AllUsers -Verbose
   3. Close powershell to complete administrative installation.   
   
   Steps to install excel module for linux (assumes pwsh already installed)
      1. launch terminal or connect with ssh
      2. sudo pwsh -c "Find-Module ImportExcel | Install-Module -Scope:AllUsers -Verbose"
   
   
   You man now run powershell or pwsh and get-vc to connect to the desired vcenter.

#>