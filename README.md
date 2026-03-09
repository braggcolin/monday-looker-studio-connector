# Monday.com → Looker Studio Community Connector

A free, open-source Looker Studio community connector for Monday.com. Dynamically connects any Monday.com board to Looker Studio without any hardcoding or manual field mapping — just your API key and board ID.

---

## Features

- **Dynamic schema discovery** — all board columns are automatically detected and typed at runtime. No manual configuration needed when your board changes.
- **Three data modes** — connect the same board three times to get Items, Subitems, and Updates (comments) as separate data sources.
- **Full column type support** — dates, checkboxes, numbers, people, links, files, timelines and auto-number columns are all handled natively.
- **People columns resolved** — person/people columns return both name and email address via a batched user lookup.
- **Link and file columns split** — each returns a `_URL` field (URL type, clickable) and a `_Value` field (display text or filename).
- **Timeline columns split** — each returns a `_Start` and `_End` date field, both properly typed as dates.
- **Comment history** — the Updates data source pulls comments from both parent items and subitems, including author name, email and timestamp.
- **Cursor-based pagination** — handles boards of any size.
- **UTC timestamps** — all datetimes returned in UTC; set your report timezone in Looker Studio via File → Report Settings → Timezone.
- **Works with any Monday.com account** — no instance-specific configuration. Compatible with any board your API key has access to.

---

## Data Sources

Set up the connector once per data type on the same board. Each connection produces a separate flat table that can be used independently or blended together in Looker Studio on `Item ID`.

| Data Type | Description |
|---|---|
| **Items** | All board items with full column data |
| **Subitems** | Child tasks with parent item context on every row |
| **Updates** | Comment history from both items and subitems |

---

## Field Types

| Monday Column Type | Looker Studio Field | Notes |
|---|---|---|
| Text, Status, Dropdown, Email, Phone, Tags | TEXT Dimension | |
| Date | YEAR_MONTH_DAY Dimension | Format: YYYYMMDD |
| Checkbox | BOOLEAN Dimension | |
| Numbers, Rating, Vote, Progress | NUMBER Metric | |
| Link | `_URL` (URL) + `_Value` (TEXT) | Split into two fields |
| File | `_URL` (URL) + `_Value` (TEXT) | Filename + Google Drive / direct URL |
| People | `_Name` (TEXT) + `_Email` (TEXT) | Comma-separated for multi-person columns |
| Timeline | `_Start` (YEAR_MONTH_DAY) + `_End` (YEAR_MONTH_DAY) | Split into two date fields |
| Auto Number | TEXT Dimension | Injected positionally, resets per group |
| Comment timestamps | YEAR_MONTH_DAY_SECOND Dimension | Full datetime in UTC |

---

## Setup

### 1. Get your Monday.com API key

1. Log into Monday.com
2. Click your avatar (top right) → **Developers**
3. Click **My Access Tokens** → **Show** → copy your personal API token

### 2. Deploy the connector in Google Apps Script

1. Go to [script.google.com](https://script.google.com) and create a new project
2. Paste the contents of `Code.gs` into the editor
3. Replace the contents of `appsscript.json` (via Project Settings → Show `appsscript.json`) with the provided file
4. Click **Deploy** → **New deployment**
5. Select type: **Add-on**
6. Click **Deploy** and copy the deployment ID

### 3. Connect in Looker Studio

1. In your Looker Studio report click **Add data** → **Build your own**
2. Search for your connector by deployment ID or name
3. Authorise the connector when prompted
4. Enter your **Monday.com API key** when asked to authenticate
5. Enter your **Board ID** (found in the board URL: `https://yourcompany.monday.com/boards/<BOARD_ID>`)
6. Select your **Data Type**: Items, Subitems, or Updates
7. Click **Connect**

### 4. Set your report timezone

All timestamps are returned in UTC. To display times in your local timezone:

**File → Report Settings → Timezone** → select your timezone

---

## Connecting Multiple Data Sources

To get Items, Subitems, and Updates for the same board, repeat the **Add data** step three times using the same Board ID but selecting a different Data Type each time. You can then blend them in Looker Studio using **Blend Data** on the `Item ID` field.

---

## Calculated Field: Clickable Links

For link and file columns, combine the two split fields into a clickable hyperlink using a Looker Studio calculated field:

```
HYPERLINK(col_mylink_url, col_mylink_value)
```

---

## Resetting your API Key

If you need to rotate your API key, run `resetAuth()` from the Apps Script editor:

**Run → Run function → resetAuth**

Then reconnect the data source in Looker Studio to enter your new key.

---

## Known Limitations

- **Explore mode** does not work with HEAD/test deployments. Use **Create Report** instead, or publish a versioned deployment for full Explore support.
- **Multiple files** — file columns with multiple attachments return the first file only.
- **Mirror and formula columns** are not currently supported and are excluded from the schema.
- **Teams** in people columns are excluded; only individual person entries are resolved.

---

## Contributing

Pull requests are welcome. If you find a bug or want to request a feature, please open an issue on GitHub.

---

## Licence

MIT — free for personal and commercial use. See [LICENSE](LICENSE) for details.
