# LDM_ #
# (Owner : ) - Local DM for the xxx. #

## Unified changelog - commit messages structure
*How to write commit messages compatible with CHANGELOG*

Each change in the system (i.e. commit) should have a change type. Set of available types is: FIX|PFIX|FEAT|TECH|QA|PERF|DOC|CONF.
Each system must have the changelog file and follow the rules of selected format. There are two supported formats for git commit and changelog messages.
 
### Strict format
`TYPE (COUNTRY) *FLAG: [XYZ-1234] Description`
 
Examples:
- `FIX (ID,IN,KZ,VN): [TIN-8810] SNMImport - ConcurrentModificationException and saving wrong translations values fix`
- `PFIX DB: [RFC-171586] Latest insurance contract status transition check`
- `FEAT (IN) COMPLETE: [PCG-260] CBL-270 Added new accounting method type CD4, update prod. profile PP022N`
 
### Rebel format
`[TYPE COUNTRY FLAG XYZ-1234] Description`
 
Optionally, you may use commas or extract ticket number to description, i.e.:
`[TYPE, COUNTRY, FLAG] (XYZ-1234) Description`
 
Examples:
- `[FIX ID IN KZ VN TIN-8810] SNMImport - ConcurrentModificationException and saving wrong translations values fix`
- `[PFIX RFC-171586 DB] Latest insurance contract status transition check`
- `[FEAT, IN, COMPLETE] PCG-260 CBL-270 Added new accounting method type CD4, update prod. profile PP022N`
 
Ticket can be further used in a TC job to close the ticket in JIRA in case of 1:1 commit-to-JIRA mapping after successful build/deploy or passing test.
 
Image below describes the whole changelog workflow:
https://git.homecredit.net/kisel/changelog-viewer/raw/master/workflow.PNG
 
## List of supported flags
 
Following is the list of flags which might be used:
- Number of the merge request or git commit hash: `!123`, `123!`, `!1e25ab5412` 
- Name of the author (should be nickname at the gitlab): `@myNickAtGit`
- Indicator for the DataBase change: `DB` 
- Indicator for the properties (deployment plan) change: `PROPS` 
- Indicator for the API change, optionally it may have names of APIs after `:`, multiple names are separated by ampersand: `API:customerRest_v1.1&soap_v2` 
