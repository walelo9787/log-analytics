# Kafka to MySql
This project plays the intermediary link between the kafka instance and MySql
database, that is why, the two servers (kafka and MySql) need to be already running before starting this application.

The main configuration for this package is set in the configuration/config.json file.

Based on that config, the program creates a consumer to connect to the specified topic in the
kafka instance to pull data from for 5 seconds (See Consumer class to modifiy the duration).

Then, the pulled records are pushed to a MySql database under the specified coordinates.

For the MySql database connection and for obvious security reasons, although it is a small project, one environment variable has to be set prior to starting this application:

- 'PASSWORD': password relative to the MySql server connection

The username is set to a default value "root" which can be updated through the config,
if one wishes to.

Also, the database specified in the config, has to be created prior to executing the program, to ensure a safe and proper connection to the MySql instance.