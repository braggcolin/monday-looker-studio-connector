# Privacy Policy

**Last updated: March 2026**

This Privacy Policy describes how the Monday.com → Looker Studio Community 
Connector ("the Connector") handles your data.

## Who operates this Connector

This Connector is developed and maintained by Colin Bragg, trading as 
Modus Transform Ltd. It is free and open-source software, provided without 
warranty or commercial support obligation.

## What data the Connector accesses

The Connector accesses data from your Monday.com account via the Monday.com 
GraphQL API, including:

- Board items, subitems, and column values
- User names and email addresses (for resolving people columns)
- Board comments and update history

The Connector only accesses boards and data that your Monday.com API key has 
permission to view.

## How your data is used

Data retrieved from Monday.com is passed directly to Google Looker Studio for 
display and analysis. It is not stored, logged, sold, or shared with any third 
party by this Connector.

## Your API key

Your Monday.com API key is stored in Google Apps Script UserProperties, scoped 
to your Google account. It is never written to any log, included in any query 
string, or shared with any party other than Monday.com for the purpose of 
authenticating API requests.

## Third-party services

This Connector interacts with the following third-party services:

- **Monday.com** — your data is retrieved from their API. See the 
  [Monday.com Privacy Policy](https://monday.com/l/privacy/).
- **Google Looker Studio** — your data is displayed within Google's platform. 
  See the [Google Privacy Policy](https://policies.google.com/privacy).

## Changes to this policy

This policy may be updated from time to time. The latest version will always 
be available at this URL. The "Last updated" date at the top of this page 
reflects the most recent revision.

## Contact

For questions about this policy, open an issue at 
[github.com/braggcolin/monday-looker-studio-connector/issues](https://github.com/braggcolin/monday-looker-studio-connector/issues).
