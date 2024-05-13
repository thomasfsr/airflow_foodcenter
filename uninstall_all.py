import subprocess

# Get the list of installed packages
installed_packages = subprocess.check_output(['pip', 'freeze']).decode('utf-8').split('\n')

# Uninstall each package
for package in installed_packages:
    if package.strip():  # Skip empty lines
        subprocess.call(['pip', 'uninstall', '-y', package.split('==')[0]])
