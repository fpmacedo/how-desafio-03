
#sudo apt update
#
#sudo apt install python3-pip
#
#sudo apt install sqlite3
#
#
#sudo apt install python3.10-venv
#
#sudo apt-get install libpq-dev
#
#sudo pip3  install virtualenv 
#
#sudo python3 -m virtualenv  /home/fcmacedo/airflow_new/venv 

#source /home/fcmacedo/airflow_new/venv/bin/activate

export AIRFLOW_HOME=/mnt/c/Users/filipe.macedo_ifood/Documents/how-desafio-03/airflow

AIRFLOW_VERSION=2.7.2

# Extract the version of Python you have installed. If you're currently using a Python version that is not supported by Airflow, you may want to set this manually.
# See above for supported versions.
PYTHON_VERSION="$(python3 --version | cut -d " " -f 2 | cut -d "." -f 1-2)"

CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
# For example this would install 2.7.2 with python 3.8: https://raw.githubusercontent.com/apache/airflow/constraints-2.7.2/constraints-3.8.txt

pip install "apache-airflow[amazon]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

#pip install 'apache-airflow[amazon]'

airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin

#airflow db upgrade

airflow db migrate
airflow webserver &
airflow scheduler