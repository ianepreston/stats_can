# Python api for Statistics Canada New Data Model (NDM)

API documentation for StatsCan can be found here: https://www.statcan.gc.ca/eng/developers/wds

If you're looking for Table/Vector IDs to use in the app you can find them through this:
https://www150.statcan.gc.ca/n1/en/type/data

I'll slowly be building this up, mostly to meet my personal needs (hence English only for now)
Right now it's functional for pulling table data into a dictionary, downloading a list of tables to a folder, checking all downloaded tables in a folder for updates, and updating if appropriate, and reading a table into a pandas DataFrame

This is my first git repo, so I don't really know how to properly package this to make it useful for others.
I'm running anaconda to build it, so if you have that it should be fine. 
If anyone with more experience wants to help out with making this functional for the broader community that would be great.
