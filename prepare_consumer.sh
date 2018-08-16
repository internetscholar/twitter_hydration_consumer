#!/bin/bash
sudo apt-get -y update
sudo apt-get -y upgrade
sudo apt-get -y install python3-pip
pip3 install --trusted-host pypi.python.org -r requirements.txt
chmod +x ~/my_script.sh
chmod 0600 ~/config.ini
crontab -l | { cat; echo "@reboot ~/my_script.sh"; } | crontab -