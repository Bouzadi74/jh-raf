@echo off
echo ========================================
echo Installation de JH-RAF
echo ========================================
echo.

echo [1/3] Verification de Python...
python --version >nul 2>&1
if errorlevel 1 (
    echo ERREUR: Python n'est pas installe ou n'est pas dans le PATH
    echo Veuillez installer Python depuis https://python.org
    pause
    exit /b 1
)
echo Python detecte avec succes !
echo.

echo [2/3] Installation des dependances...
echo Installation de pandas, PyQt5, openpyxl, matplotlib...
pip install -r requirements.txt
if errorlevel 1 (
    echo ERREUR: Impossible d'installer les dependances
    echo Verifiez votre connexion internet et relancez le script
    pause
    exit /b 1
)
echo Dependances installees avec succes !
echo.

echo [3/3] Creation du raccourci sur le bureau...

:: Chemin vers Python (ajustez selon votre installation)
set PYTHON_PATH=python

:: Chemin vers l'application
set APP_PATH=%~dp0gui\app.py

:: Nom du raccourci
set SHORTCUT_NAME=JH-RAF.lnk

:: Chemin du bureau
set DESKTOP_PATH=%USERPROFILE%\Desktop

:: Créer le raccourci avec PowerShell
powershell -Command "$WshShell = New-Object -comObject WScript.Shell; $Shortcut = $WshShell.CreateShortcut('%DESKTOP_PATH%\%SHORTCUT_NAME%'); $Shortcut.TargetPath = '%PYTHON_PATH%'; $Shortcut.Arguments = '%APP_PATH%'; $Shortcut.WorkingDirectory = '%~dp0'; $Shortcut.IconLocation = '%~dp0icon.ico'; $Shortcut.Description = 'Application JH-RAF - Consommation de charge et RAF'; $Shortcut.Save()"

if errorlevel 1 (
    echo ERREUR: Impossible de creer le raccourci
    pause
    exit /b 1
)

echo.
echo ========================================
echo Installation terminee avec succes !
echo ========================================
echo.
echo Le raccourci JH-RAF a ete cree sur votre bureau.
echo Vous pouvez maintenant lancer l'application !
echo.
pause
