# tidypolis
A package to simplify downloading, caching and updating POLIS data

# Usage
First make sure you have `devtools` installed. Then run the following command: 

```
devtools::install_github("nish-kishore/tidypolis")
```

After you can use any function developed in this package using 

```
tidypolis::{function}
```

# Quick start 
- Use the `tidypolis::init_tidypolis()` function to create a local folder to store POLIS
- This function will ask for your POLIS API key so please have this ready
- Use the `tidypolis::get_polis_data()` to pull all POLIS data


## Release Schedule
- V1 to be released 2/1/2024, you can track progress [here](https://github.com/nish-kishore/tidypolis/milestone/1).

Key features to be released include:

1. Set-up of GitHub Flow
2. User documentation 
3. Bug identification for fresh run
4. Additional change logging
5. Population download functionality
