# tidypolis
A package to simplify downloading, caching and updating POLIS data. Functions 
to clarify agency specific pre-processing and data cleaning steps are also included. 

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
- Use the `tidypolis::init_tidypolis()` function to create a folder to store and update POLIS data. 
You must specify if you wish the folder to be stored locally or on the EDAV environment by setting 
the `edav` parameter in the function to `T` or `F`. 
- This function will ask for your POLIS API key so please have this ready
- Use the `tidypolis::get_polis_data()` to pull all POLIS data and reconcile any missing or updated information, 
the data will be stored in the local folder or in the EDAV environment you've specified. 
- Use `tidypolis::preprocess_data(type = "cdc")` to process all POLIS data with cleaning methodologies 
used by the SIR team. This requires access to the CDC EDAV environment. If you do not have this access please 
contact us to get the pre-requisite data files to run this process locally. 


## Release Schedule

- V1.1 to be released 4/1/2024, you can track progress [here](https://github.com/nish-kishore/tidypolis/milestone/2).

Key features to be released are in development and will be shared shortly. 

## Contact 
Please send a message to Nishant Kishore are ynm2@cdc.gov for any questions or queries. 

## Public Domain Standard Notice
This repository constitutes a work of the United States Government and is not
subject to domestic copyright protection under 17 USC § 105. This repository is in
the public domain within the United States, and copyright and related rights in
the work worldwide are waived through the [CC0 1.0 Universal public domain dedication](https://creativecommons.org/publicdomain/zero/1.0/).
All contributions to this repository will be released under the CC0 dedication. By
submitting a pull request you are agreeing to comply with this waiver of
copyright interest.

## License Standard Notice
The repository utilizes code licensed under the terms of the Apache Software
License and therefore is licensed under ASL v2 or later.

This source code in this repository is free: you can redistribute it and/or modify it under
the terms of the Apache Software License version 2, or (at your option) any
later version.

This source code in this repository is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE. See the Apache Software License for more details.

You should have received a copy of the Apache Software License along with this
program. If not, see http://www.apache.org/licenses/LICENSE-2.0.html

The source code forked from other open source projects will inherit its license.

## Privacy Standard Notice
This repository contains only non-sensitive, publicly available data and
information. All material and community participation is covered by the
[Disclaimer](https://github.com/CDCgov/template/blob/master/DISCLAIMER.md)
and [Code of Conduct](https://github.com/CDCgov/template/blob/master/code-of-conduct.md).
For more information about CDC's privacy policy, please visit [http://www.cdc.gov/other/privacy.html](https://www.cdc.gov/other/privacy.html).

## Contributing Standard Notice
Anyone is encouraged to contribute to the repository by [forking](https://help.github.com/articles/fork-a-repo)
and submitting a pull request. (If you are new to GitHub, you might start with a
[basic tutorial](https://help.github.com/articles/set-up-git).) By contributing
to this project, you grant a world-wide, royalty-free, perpetual, irrevocable,
non-exclusive, transferable license to all users under the terms of the
[Apache Software License v2](http://www.apache.org/licenses/LICENSE-2.0.html) or
later.

All comments, messages, pull requests, and other submissions received through
CDC including this GitHub page may be subject to applicable federal law, including but not limited to the Federal Records Act, and may be archived. Learn more at [http://www.cdc.gov/other/privacy.html](http://www.cdc.gov/other/privacy.html).

## Records Management Standard Notice
This repository is not a source of government records, but is a copy to increase
collaboration and collaborative potential. All government records will be
published through the [CDC web site](http://www.cdc.gov).


