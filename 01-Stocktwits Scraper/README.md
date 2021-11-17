# Stocktwits-Scraper
Simple Stocktwits Scraper

This scraper is based on the work of mtmmy (https://github.com/mtmmy/stock-twits-scraper) and it has been slightly modified so it can be used as a Google Cloud Function. 
Te code is implemented in Python 3.8. 

It collects twits from the page https://stocktwits.com/ for a specific stock that must be indicated, from an initial ID to an indicated date. 

Since Google Cloud functions run for a maximum of nine minutes, this code saves the ID of the last collected twit in the file name. So, the following run can read the last file name to obtain the ID from which to begin collecting.

The function is designed to work with Google Cloud Scheduller launching a new instance every ten minutes and setting the maximum number of simultaneous instances to 1.

