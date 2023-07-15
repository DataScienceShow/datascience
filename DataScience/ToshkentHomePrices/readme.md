![Tashkent Home Price Prediction](https://github.com/DataScienceShow/datascience/blob/1e4e899c0102218f77bf6bf6c6079aa83de2c992/DataScience/ToshkentHomePrices/2023-07-15%2016_46_30-Tashkent%20Home%20Price%20Prediction.png)

This data science project series walks through step by step process of how to build a real estate price prediction website. We will first build a model using sklearn and linear regression using Tashkent home prices dataset from scraped files (https://github.com/DataScienceShow/datascience/blob/e11046da169b0e3781642d10400188ec170ec1ce/Web%20Scraping/Olx%20webscraping/new%20building.csv). Second step would be to write a python flask server that uses the saved model to serve http requests. Third component is the website built in html, css and javascript that allows user to enter home square Meter area,rooms etc and it will call python flask server to retrieve the predicted price. During model building we will cover almost all data science concepts such as data load and cleaning, outlier detection and removal, feature engineering, dimensionality reduction, gridsearchcv for hyperparameter tunning, k fold cross validation etc. Technology and tools wise this project covers.I also created Power BI report that shows Toshkent Home Prices Analysis

1. Python
2. Numpy and Pandas for data cleaning
3. Matplotlib for data visualization
4. Sklearn for model building
5. Jupyter notebook, visual studio code and pycharm as IDE
6. Python flask for http server
7. HTML/CSS/Javascript for UI
8. Power BI report https://app.powerbi.com/view?r=eyJrIjoiZjE1MTRkYzktZTAzMC00OTczLWJlNTYtZjQ4OTFiYWEwZWEwIiwidCI6ImExNzVjMDkyLWQ4NTgtNGZhMS1hN2ZiLThmYTU3M2IwNjJmYiIsImMiOjEwfQ%3D%3D

# Deploy this app to cloud (AWS EC2)

1. Create EC2 instance using amazon console, also in security group add a rule to allow HTTP incoming traffic
2. Now connect to your instance using a command like this,
```ssh -i "C:\Data Scients\Learn Data Science\Learn MachineLearning\Olx End to End Project\Olx_THP\amazon ec2 key-pair\first_instance.pem" ubuntu@ec2-16-171-169-211.eu-north-1.compute.amazonaws.com
```
3. nginx setup
   1. Install nginx on EC2 instance using these commands,
   ```
   sudo apt-get update
   sudo apt-get install nginx
   ```
   2. Above will install nginx as well as run it. Check status of nginx using
   ```
   sudo service nginx status
   ```
   3. Here are the commands to start/stop/restart nginx
   ```
   sudo service nginx start
   sudo service nginx stop
   sudo service nginx restart
   ```
   4. Now when you load cloud url in browser you will see a message saying "welcome to nginx" This means your nginx is setup and running.
4. Now you need to copy all your code to EC2 instance. You can do this either using git or copy files using winscp. We will use winscp. You can download winscp from here: https://winscp.net/eng/download.php
5. Once you connect to EC2 instance from winscp (instruction in a youtube video), you can now copy all code files into /home/ubuntu/ folder. The full path of your root folder is now: **/home/ubuntu/Olx_THP_org/**
6.  After copying code on EC2 server now we can point nginx to load our property website by default. For below steps,
    1. Create this file /etc/nginx/sites-available/bhp.conf. The file content looks like this,
    ```
    server{
    listen 80;
        server_name tashkenthomeprice;
        root /var/www/html/client;
        index app.html;
        location /api/ {
             rewrite ^/api(.*) $1 break;
             proxy_pass http://127.0.0.1:5000;
        }
    }
    ```
    2. Create symlink for this file in /etc/nginx/sites-enabled by running this command,
    ```
    sudo ln -v -s /etc/nginx/sites-available/tashkenthomeprice.conf
    ```
    3. Remove symlink for default file in /etc/nginx/sites-enabled directory,
    ```
    sudo unlink default
    ```
    4. Restart nginx,
    ```
    sudo service nginx restart
    ```
7. Now install python packages and start flask server
```
sudo apt-get install python3-pip
sudo pip3 install -r /home/ubuntu/Olx_THP_org/server/server/requirements.txt
python3 /home/ubuntu/Olx_THP_org/server/client/server.py
```
Running last command above will prompt that server is running on port 5000.
8. Now just load your cloud url in browser (for me it was http://ec2-16-171-169-211.eu-north-1.compute.amazonaws.com/) and this will be fully functional website running in production cloud environment



