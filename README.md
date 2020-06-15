# super-potato
CBOE and FINRA shorting activity monitor

## Functionalities:
1) Download short sale data from CBOE and FINRA
2) Calculate short volume ratio(shortvolume/totalvolume) for several exchanges/darkpools
3) Store calculated data into database(sqlite) 
4) Automatically update each day by creating a .bat file and task scheduler calling the scripts

## Usage:
1) Clone the repo
2) Edit config file
3) Run setup.py OR run cboeData.py and/or finraData.py with --setup flag
