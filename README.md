# fabric-sharepoint-ingestion
This repository contains a script to extract CSV files from SharePoint and store them in a Lakehouse on Microsoft Fabric using Python, PySpark, and Microsoft Graph API.

# Features
Authentication via Microsoft Graph API to access SharePoint files
Listing and downloading CSV files from a specific folder
Processing and cleaning data with Pandas and PySpark
Writing data to Delta Tables in Microsoft Fabric

# Technologies Used
Python (requests, pandas, io)
PySpark (Spark DataFrame, SQL, Delta Lake)
Microsoft Graph API (for authentication and SharePoint access)
Microsoft Fabric (data storage)

# How It Works
Authenticates with Microsoft Graph API and retrieves an access token
Finds the Site ID and Drive ID of SharePoint
Lists and downloads the CSV files from the specified folder
Converts the files into a Spark DataFrame, applies cleaning, and transforms the data
Saves the processed data as a Delta Table in Microsoft Fabric

# Usage
Configure the authentication parameters and SharePoint details (tenant_id, client_id, client_secret, site_name, etc.), and the script will automatically process the CSV files and store them in Microsoft Fabric.
