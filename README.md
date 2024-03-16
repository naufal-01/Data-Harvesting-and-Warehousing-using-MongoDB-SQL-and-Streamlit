# Guvi_capstone_project1 : Data-Harvesting-and-Warehousing-using-MongoDB-SQL-and-Streamlit
The YouTube Data Harvesting and Warehousing project is aimed at collecting data from YouTube channels, storing it in a MongoDB database, and then migrating it to a PostgreSQL database for further analysis. The project involves extracting information such as channel details, video information, and comments from YouTube channels using the YouTube Data API. This data is then stored in a structured format in both MongoDB and PostgreSQL databases to facilitate querying and analysis.

LinkedIn : https://www.linkedin.com/in/mohamed-naufal/

Tools and Libraries Used:
Python: The project is implemented using the Python programming language, providing flexibility and ease of development.

MongoDB: MongoDB is used as a NoSQL database to store the extracted YouTube data. It offers flexibility in handling unstructured data and allows for easy scaling.

PostgreSQL: PostgreSQL is utilized as a relational database management system for warehousing the YouTube data. It provides robust SQL querying capabilities and ensures data integrity.

Google API Client Library: The googleapiclient library is used to interact with the YouTube Data API, enabling the retrieval of channel information, video details, and comments.

Pandas: Pandas is used for data manipulation and analysis, facilitating tasks such as cleaning, transforming, and organizing the extracted data.

Streamlit: Streamlit is utilized to create an interactive web application for users to interact with the collected YouTube data, visualize insights, and execute SQL queries.

Required Libraries:

googleapiclient: To interact with the YouTube Data API.

pymongo: To connect to and manipulate MongoDB databases.

psycopg2: To interact with PostgreSQL databases.

pandas: For data manipulation and analysis.

streamlit: For building the interactive web application interface

Features:

Data Collection: The project allows for the extraction of channel information, video details, and comments from YouTube channels using the YouTube Data API.

Data Storage: Extracted data is stored in both MongoDB and PostgreSQL databases, providing flexibility and scalability in data management.

SQL Migration: Data stored in MongoDB is migrated to a PostgreSQL database, enabling relational querying and analysis.

Interactive Web Interface: The project includes a Streamlit web application that allows users to interact with the collected YouTube data, visualize insights, and execute SQL queries.

SQL Querying: Users can execute SQL queries to retrieve specific information from the PostgreSQL database, such as channel statistics, video analytics, and comment insights.

Data Visualization: The Streamlit interface provides features for visualizing data, including tables, charts, and graphs, to facilitate data exploration and interpretation.
