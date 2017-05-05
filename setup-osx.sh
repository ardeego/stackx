brew update
brew install mysql
brew tap homebrew/services
brew services start mysql
brew services list
mysqladmin -u root password 'your-password'
#brew install mysql-connector-c
pip install mysqlclient

mysql -u root -p
FLUSH PRIVILEGES;
ALTER USER 'root'@'localhost' IDENTIFIED BY 'YOUR-PW'
CREATE USER 'stackx'@'localhost' IDENTIFIED BY 'YOUR-PW';