This package is designed to contain all pieces necessary to manage a Streamer Reward Program.


Dependencies:
https://github.com/Tsagh/OPTwitchData
Requirements:
Heroku PostgreSQL

Required Database Schema:
Programs (program_id SERIAL PRIMARY KEY, programName varchar(100) NOT NULL, sponsor varchar(100) NOT NULL)
Streamers (streamer_id SERIAL PRIMARY KEY, email varchar(100) NOT NULL, name varchar(50) NOT NULL, channel varchar(50) NOT NULL)
ProgramRewards (code varchar(100) NOT NULL, program_id int NOT NULL, description varchar(100) NOT NULL, streamer_id int NULL, dateAwarded timestamp, isAssigned boolean NOT NULL, wasSent boolean NOT NULL, PRIMARY KEY(code), FOREIGN KEY(streamer_id) REFERENCES Streamers, FOREIGN KEY (program_id) REFERENCES Programs)
ProgramParticipants ( program_id int NOT NULL, streamer_id int NOT NULL, game varchar(100) NOT NULL, ign varchar(100) NOT NULL, status varchar(100) NOT NULL, FOREIGN KEY (program_id) REFERENCES Programs, FOREIGN KEY (streamer_id) REFERENCES Streamers)
ProgramRules(program_id int NOT NULL, metrics varchar(120) NOT NULL, operands varchar(120) NOT NULL, limits varchar(120) NOT NULL, frequency varchar(50) NOT NULL, reward varchar(120) NOT NULL, FOREIGN KEY (program_id) REFERENCES Programs)