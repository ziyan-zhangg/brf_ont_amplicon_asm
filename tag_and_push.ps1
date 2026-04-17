$VER = "v2.03"
$DATE = "Date: {0}" -f (Get-Date)

$COMMENT = @"
* New: CLean up folder available only at Gadi.
"@

Move-Item -Path "changelog.txt" -Destination "changelog_old.txt"
Set-Content -Path "changelog.txt" -Value $VER
Add-Content -Path "changelog.txt" -Value $DATE
Add-Content -Path "changelog.txt" -Value $COMMENT
Add-Content -Path "changelog.txt" -Value ""
Get-Content -Path "changelog_old.txt" | Add-Content -Path "changelog.txt"
Remove-Item -Path "changelog_old.txt"

git add .                        # Changed from -u to catch new files too
$MSG = $COMMENT
git commit -m $MSG
git tag -a $VER -m $MSG

git push -u origin main          # -u sets upstream tracking on first push
git push origin $VER             # Push the tag