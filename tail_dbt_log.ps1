# https://www.red-gate.com/simple-talk/sysadmin/powershell/how-to-use-parameters-in-powershell/

param ([int] $row=1000)
Get-Content -Path "C:\Users\chuong.nguyenh2\Documents\dbt_oracle_d5\logs\dbt.log" -Tail $row -Wait

# run script    : .\tail_dbt_log.ps1 [-Tail 1000]
# or            : .\tail_dbt_log.ps1 [1000]
