// =============================================================================
// Monday.com → Looker Studio Community Connector
// =============================================================================
//
// PURPOSE
//   Exposes a Monday.com board as a Looker Studio data source. All board
//   columns are discovered dynamically at runtime; data types are inferred
//   from Monday.com column type metadata.
//
// AUTHENTICATION
//   Uses Looker Studio's built-in KEY auth flow. The API key is stored in
//   Apps Script UserProperties and is NEVER written to any log.
//
//   To manually clear a stored key (e.g. when rotating keys) run resetAuth()
//   from the Apps Script editor: Run → resetAuth.
//
// MONDAY.COM API
//   Uses the Monday.com GraphQL API v2 with cursor-based pagination.
//   Update MONDAY_API_VERSION below when Monday releases a new stable version.
//
// KNOWN LIMITATION
//   Looker Studio's "Explore" feature does not work with HEAD/test deployments.
//   Use "Create Report" instead, which works correctly. For a versioned
//   deployment, Explore will also work.
//
// DEVELOPER NOTES
//   • All private helper functions end with an underscore (_) by convention.
//   • Looker Studio expects getAuthType / getConfig / getSchema / getData
//     to be present at the top level (no class/namespace wrapper).
//   • CacheService is used to avoid redundant schema fetches within a session.
//   • Column field types are inferred from Monday.com column types:
//     dates → YEAR_MONTH_DAY, checkboxes → BOOLEAN, numbers → NUMBER (Metric),
//     links/files → split into _URL + _Value, people → split into _Name + _Email.
//     All other columns are TEXT Dimensions.
//
// =============================================================================


// -----------------------------------------------------------------------------
// GLOBAL CONSTANTS  (update these when instance or API version changes)
// -----------------------------------------------------------------------------

/**
 * Monday.com GraphQL API endpoint.
 * Always api.monday.com/v2 — the subdomain does not route API traffic.
 */
var MONDAY_API_URL = 'https://api.monday.com/v2';

/**
 * Monday.com API version header.
 * Update when a new stable version is published:
 * https://developer.monday.com/api-reference/docs/api-versioning
 */
var MONDAY_API_VERSION = '2024-01';

/**
 * Items per paginated API call. Hard maximum enforced by Monday.com is 500.
 * Reduce if you encounter query-complexity errors on very wide boards.
 */
var PAGE_LIMIT = 500;

/** UserProperties key for the stored API token. */
var API_KEY_PROPERTY = 'MONDAY_API_KEY';

/** CacheService TTL in seconds for board column metadata. */
var COLUMN_CACHE_TTL = 600;

/**
 * Column types that are skipped when building the schema.
 * 'name'     – Always returned by Monday but is a reserved field ID in Looker
 *              Studio; already covered by the synthetic 'item_name' field.
 * 'subtasks' – Returns sub-item IDs, not a scalar value.
 * 'button'   – UI-only element, carries no data.
 * 'doc'      – Embedded doc reference, no exportable scalar value.
 */
var SKIP_COLUMN_TYPES = {
  name    : true,
  subtasks: true,
  button  : true,
  doc     : true
};


// =============================================================================
// SECTION 1 – AUTHENTICATION
// =============================================================================

/**
 * Returns the authentication type for this connector.
 * Looker Studio renders a single "API Key" field in the credentials dialog.
 *
 * @returns {GetAuthTypeResponse}
 */
function getAuthType() {
  var cc = DataStudioApp.createCommunityConnector();
  return cc
    .newAuthTypeResponse()
    .setAuthType(cc.AuthType.KEY)
    .build();
}

/**
 * Validates and stores the API key submitted by the user.
 * The key is tested against the Monday.com API before being persisted.
 *
 * @param  {Object} request  Looker Studio credentials request (request.key).
 * @returns {SetCredentialsResponse}
 */
function setCredentials(request) {
  var cc      = DataStudioApp.createCommunityConnector();
  var isValid = validateApiKey_(request.key);

  if (!isValid) {
    return cc.newSetCredentialsResponse().setIsValid(false).build();
  }

  // Store in UserProperties — scoped to the current Google account,
  // never shared with other users, never written to any log.
  PropertiesService.getUserProperties().setProperty(API_KEY_PROPERTY, request.key);

  return cc.newSetCredentialsResponse().setIsValid(true).build();
}

/**
 * Returns true if a stored key exists and is still accepted by Monday.com.
 * Looker Studio calls this on every connector load.
 *
 * @returns {boolean}
 */
function isAuthValid() {
  var key = PropertiesService.getUserProperties().getProperty(API_KEY_PROPERTY);
  if (!key) return false;
  return validateApiKey_(key);
}

/**
 * Deletes the stored Monday.com API key for the current user.
 *
 * MANUAL USE: Run → resetAuth in the Apps Script editor to force
 * re-authentication on next connector use (e.g. when rotating API keys).
 */
function resetAuth() {
  PropertiesService.getUserProperties().deleteProperty(API_KEY_PROPERTY);
  Logger.log('[Monday Connector] API key cleared. Re-authentication required on next use.');
}

/**
 * Validates an API key with a lightweight Monday.com API probe.
 * The key is only ever placed in the HTTP Authorization header — never logged.
 *
 * @param  {string}  key  Monday.com API token to validate.
 * @returns {boolean}
 */
function validateApiKey_(key) {
  try {
    var result = callMondayApi_(key, '{ me { id name } }');
    return !!(result && result.data && result.data.me && result.data.me.id);
  } catch (e) {
    Logger.log('[Monday Connector] Key validation failed: ' + e.message);
    return false;
  }
}


// =============================================================================
// SECTION 2 – COMPATIBILITY & CONFIGURATION
// =============================================================================

/**
 * Confirms the connector is compatible with the current Looker Studio version.
 * Required by some Looker Studio runtime versions before getData is called.
 *
 * @returns {CheckCompatibilityResponse}
 */
function checkCompatibility(request) {
  return DataStudioApp
    .createCommunityConnector()
    .newCheckCompatibilityResponse()
    .setIsCompatible(true)
    .build();
}

/**
 * Returns the connector configuration form (shown after authentication).
 * The user supplies the numeric Monday.com board ID from the board URL:
 *   https://<instance>/boards/<BOARD_ID>
 *
 * @returns {GetConfigResponse}
 */
function getConfig(request) {
  var config = DataStudioApp.createCommunityConnector().getConfig();

  config
    .newInfo()
    .setId('connectorInfo')
    .setText(
      'Connect to a Monday.com board to use its data in Looker Studio.\n\n' +
      'Find your Board ID in the board URL: https://yourcompany.monday.com/boards/<BOARD_ID>\n\n' +
      'You need at least "View" access to the board in Monday.com.\n\n' +
      '⚠ TIMEZONE NOTE: All timestamps (e.g. comment dates) are returned in UTC. ' +
      'To display times in your local timezone, set the report timezone in ' +
      'Looker Studio via File → Report Settings → Timezone.'
    );

  config
    .newTextInput()
    .setId('boardId')
    .setName('Board ID')
    .setHelpText(
      'The numeric ID of the Monday.com board, e.g. 1234567890. ' +
      'Copy it from the board URL shown above.'
    )
    .setPlaceholder('1234567890')
    .setAllowOverride(false);

  // Data type selection — determines which table this data source returns.
  // Set up the connector once per data type on the same board.
  config
    .newSelectSingle()
    .setId('dataType')
    .setName('Data Type')
    .setHelpText(
      'Select "Items" to pull board item data (columns and values), or ' +
      '"Updates" to pull the comment/activity history for each item.'
    )
    .addOption(config.newOptionBuilder().setLabel('Items').setValue('items'))
    .addOption(config.newOptionBuilder().setLabel('Subitems').setValue('subitems'))
    .addOption(config.newOptionBuilder().setLabel('Updates (Comments)').setValue('updates'))
    .setAllowOverride(false);

  config.setDateRangeRequired(false);

  return config.build();
}


// =============================================================================
// SECTION 3 – SCHEMA
// =============================================================================

/**
 * Returns the schema (field definitions) for the selected board.
 *
 * @param  {Object} request  Contains request.configParams.boardId.
 * @returns {GetSchemaResponse}
 */
function getSchema(request) {
  var dataType = request.configParams && request.configParams.dataType
    ? request.configParams.dataType
    : 'items';

  var fields = (dataType === 'updates')
    ? buildUpdatesFields_()
    : (dataType === 'subitems')
      ? buildSubitemsFields_(request)
      : buildFields_(request);

  return DataStudioApp
    .createCommunityConnector()
    .newGetSchemaResponse()
    .setFields(fields)
    .build();
}

/**
 * Builds the Looker Studio Fields collection for a board.
 *
 * Three synthetic fields are always included:
 *   • Item ID    – unique identifier of the item
 *   • Item Name  – primary name/title of the item
 *   • Group Name – the group (section) the item belongs to
 *
 * All fields are TEXT Dimensions. Looker Studio silently rejects schemas
 * where a Dimension carries a non-TEXT type; users can cast fields in reports.
 *
 * Duplicate field IDs (after sanitisation) have '_2' appended.
 *
 * @param  {Object} request  Looker Studio request with configParams.
 * @returns {Fields}
 */
function buildFields_(request) {
  var cc     = DataStudioApp.createCommunityConnector();
  var fields = cc.getFields();
  var types  = cc.FieldType;

  var boardId = request.configParams.boardId;
  var columns = fetchBoardColumns_(boardId);

  var agg = cc.AggregationType;

  // ── Synthetic fields ───────────────────────────────────────────────────────
  fields.newDimension().setId('item_id')   .setName('Item ID')   .setType(types.TEXT)
    .setDescription('Unique identifier of the Monday.com item.');
  fields.newDimension().setId('item_name') .setName('Item Name') .setType(types.TEXT)
    .setDescription('Display name / title of the Monday.com item.');
  fields.newDimension().setId('group_name').setName('Group Name').setType(types.TEXT)
    .setDescription('Name of the Monday.com group (section) the item belongs to.');

  // Record Count metric — always returns 1 per row so Looker Studio can SUM
  // it to display a record count, consistent with other community connectors.
  fields.newMetric().setId('record_count').setName('Record Count')
    .setType(types.NUMBER)
    .setAggregation(agg.SUM)
    .setDescription('Count of Monday.com items.');

  var registeredIds = { item_id: true, item_name: true, group_name: true, record_count: true };

  // ── Board columns ──────────────────────────────────────────────────────────
  columns.forEach(function (col) {
    if (SKIP_COLUMN_TYPES[col.type]) return;

    var fieldId     = deduplicateFieldId_(sanitiseFieldId_(col.id), registeredIds);
    var displayName = sanitiseDisplayName_(col.title);
    registeredIds[fieldId] = true;

    // Link and file columns are each split into two fields:
    //   _url   – the raw URL (URL type, clickable in Looker Studio)
    //   _value – the display text / filename (TEXT type)
    // Users can combine them with HYPERLINK(<fieldId>_url, <fieldId>_value)
    // in a Looker Studio calculated field.
    //
    // link column : _value = link display text, _url = hyperlink URL
    // file column : _value = filename (e.g. myfile.pdf), _url = file URL
    if (col.type === 'link' || col.type === 'file') {
      var urlFieldId   = deduplicateFieldId_(fieldId + '_url',   registeredIds);
      var valueFieldId = deduplicateFieldId_(fieldId + '_value', registeredIds);
      registeredIds[urlFieldId]   = true;
      registeredIds[valueFieldId] = true;

      fields
        .newDimension()
        .setId(urlFieldId)
        .setName(displayName + '_URL')
        .setDescription('URL for the ' + displayName + ' column.')
        .setType(types.URL);

      fields
        .newDimension()
        .setId(valueFieldId)
        .setName(displayName + '_Value')
        .setDescription('Display name / filename for the ' + displayName + ' column.')
        .setType(types.TEXT);

      return; // skip the default field registration below
    }

    // Timeline columns are split into _Start and _End date fields, both
    // typed as YEAR_MONTH_DAY so Looker Studio treats them as proper dates.
    if (col.type === 'timeline') {
      var startFieldId = deduplicateFieldId_(fieldId + '_start', registeredIds);
      var endFieldId   = deduplicateFieldId_(fieldId + '_end',   registeredIds);
      registeredIds[startFieldId] = true;
      registeredIds[endFieldId]   = true;
      fields.newDimension().setId(startFieldId)
        .setName(displayName + '_Start').setType(types.YEAR_MONTH_DAY)
        .setDescription('Start date for the ' + displayName + ' timeline column.');
      fields.newDimension().setId(endFieldId)
        .setName(displayName + '_End').setType(types.YEAR_MONTH_DAY)
        .setDescription('End date for the ' + displayName + ' timeline column.');
      return;
    }

    // People columns are split into _Name (comma-separated display names)
    // and _Email (comma-separated email addresses resolved via user lookup).
    if (col.type === 'people') {
      var nameFieldId  = deduplicateFieldId_(fieldId + '_name',  registeredIds);
      var emailFieldId = deduplicateFieldId_(fieldId + '_email', registeredIds);
      registeredIds[nameFieldId]  = true;
      registeredIds[emailFieldId] = true;

      fields
        .newDimension()
        .setId(nameFieldId)
        .setName(displayName + '_Name')
        .setDescription('Comma-separated names for the ' + displayName + ' people column.')
        .setType(types.TEXT);

      fields
        .newDimension()
        .setId(emailFieldId)
        .setName(displayName + '_Email')
        .setDescription('Comma-separated emails for the ' + displayName + ' people column.')
        .setType(types.TEXT);

      return; // skip the default field registration below
    }

    // Assign the appropriate Looker Studio field type based on Monday column type:
    //   date     → YEAR_MONTH_DAY (Dimension) — enables date filtering/formatting
    //   checkbox → BOOLEAN (Dimension)         — true/false values
    //   numeric  → NUMBER (Metric)             — enables aggregation in charts
    //   all else → TEXT (Dimension)
    var NUMERIC_TYPES = {
      numbers : true,
      numeric : true,
      rating  : true,
      vote    : true,
      progress: true
    };

    if (col.type === 'date') {
      fields
        .newDimension()
        .setId(fieldId)
        .setName(displayName)
        .setDescription('Monday.com column type: ' + col.type)
        .setType(types.YEAR_MONTH_DAY);

    } else if (col.type === 'checkbox') {
      fields
        .newDimension()
        .setId(fieldId)
        .setName(displayName)
        .setDescription('Monday.com column type: ' + col.type)
        .setType(types.BOOLEAN);

    } else if (NUMERIC_TYPES[col.type]) {
      // NUMBER must be a Metric — NUMBER on a Dimension causes silent schema
      // rejection in Looker Studio.
      fields
        .newMetric()
        .setId(fieldId)
        .setName(displayName)
        .setDescription('Monday.com column type: ' + col.type)
        .setType(types.NUMBER);

    } else {
      fields
        .newDimension()
        .setId(fieldId)
        .setName(displayName)
        .setDescription('Monday.com column type: ' + col.type)
        .setType(types.TEXT);
    }
  });

  return fields;
}


// =============================================================================
// SECTION 4 – DATA
// =============================================================================

/**
 * Fetches data rows from Monday.com and returns them to Looker Studio.
 * Only the fields explicitly requested by Looker Studio are returned.
 *
 * When Looker Studio sends sampleExtraction:true (e.g. during schema preview),
 * only the first 10 items are returned for speed.
 *
 * @param  {Object} request  Looker Studio data request.
 * @returns {GetDataResponse}
 */
function getData(request) {
  try {
    var cc        = DataStudioApp.createCommunityConnector();
    var boardId  = request.configParams ? request.configParams.boardId  : null;
    var dataType = request.configParams ? request.configParams.dataType : 'items';
    dataType     = dataType || 'items';

    // If boardId is absent (can happen during Looker Studio's Explore handshake)
    // return a valid empty response rather than throwing, which would produce
    // the "Something went wrong" popup.
    if (!boardId) {
      return cc.newGetDataResponse().setFields(cc.getFields()).build();
    }

    // Route to the appropriate data handler based on dataType
    if (dataType === 'updates') {
      return getUpdatesData_(request, cc, boardId);
    }
    if (dataType === 'subitems') {
      return getSubitemsData_(request, cc, boardId);
    }
    if (dataType === 'subitems') {
      return getSubitemsData_(request, cc, boardId);
    }

    // ── Resolve requested fields ─────────────────────────────────────────────
    var allFields    = buildFields_(request);
    var requestedIds = (request.fields && request.fields.length > 0)
      ? request.fields.map(function (f) { return f.name; })
      : allFields.asArray().map(function (f) { return f.getId(); });

    var fieldsSubset = allFields.forIds(requestedIds);

    // ── Build Monday column → Looker field ID maps ───────────────────────────
    // Must use identical sanitisation logic as buildFields_ so IDs match.
    var columns        = fetchBoardColumns_(boardId);
    var registeredIds  = { item_id: true, item_name: true, group_name: true };
    var colIdToFieldId = {};
    var colIdToType    = {};

    // colIdToUrlFieldId   : Monday column ID → _url field ID (link/file columns)
    // colIdToValueFieldId : Monday column ID → _value field ID (link/file columns)
    // colIdToNameFieldId  : Monday column ID → _name field ID (people columns)
    // colIdToEmailFieldId : Monday column ID → _email field ID (people columns)
    var colIdToUrlFieldId   = {};
    var colIdToValueFieldId = {};
    var colIdToNameFieldId   = {};
    var colIdToEmailFieldId  = {};
    var colIdToStartFieldId  = {};
    var colIdToEndFieldId    = {};

    columns.forEach(function (col) {
      if (SKIP_COLUMN_TYPES[col.type]) return;
      var fid = deduplicateFieldId_(sanitiseFieldId_(col.id), registeredIds);
      registeredIds[fid]     = true;
      colIdToFieldId[col.id] = fid;
      colIdToType[col.id]    = col.type;
      if (col.type === 'link' || col.type === 'file') {
        colIdToUrlFieldId[col.id]   = deduplicateFieldId_(fid + '_url',   registeredIds);
        colIdToValueFieldId[col.id] = deduplicateFieldId_(fid + '_value', registeredIds);
        registeredIds[colIdToUrlFieldId[col.id]]   = true;
        registeredIds[colIdToValueFieldId[col.id]] = true;
      }
      if (col.type === 'people') {
        colIdToNameFieldId[col.id]  = deduplicateFieldId_(fid + '_name',  registeredIds);
        colIdToEmailFieldId[col.id] = deduplicateFieldId_(fid + '_email', registeredIds);
        registeredIds[colIdToNameFieldId[col.id]]  = true;
        registeredIds[colIdToEmailFieldId[col.id]] = true;
      }
      if (col.type === 'timeline') {
        colIdToStartFieldId[col.id] = deduplicateFieldId_(fid + '_start', registeredIds);
        colIdToEndFieldId[col.id]   = deduplicateFieldId_(fid + '_end',   registeredIds);
        registeredIds[colIdToStartFieldId[col.id]] = true;
        registeredIds[colIdToEndFieldId[col.id]]   = true;
      }
    });

    // ── Fetch items ───────────────────────────────────────────────────────────
    // For sample extraction Looker Studio only needs a few rows — fetch one
    // small page rather than paginating the entire board.
    var isSample     = (request.scriptParams &&
                        request.scriptParams.sampleExtraction === true);
    var items        = isSample ? fetchSampleItems_(boardId) : fetchAllItems_(boardId);
    var rowsToRender = isSample ? items.slice(0, 10) : items;

    // ── Build response ────────────────────────────────────────────────────────
    var dataResponse = cc.newGetDataResponse();
    dataResponse.setFields(fieldsSubset);

    // Build user lookup map (id → {name, email}) for people column resolution.
    // Only fetched if the board has people columns.
    var userLookup = {};
    var hasPeopleColumns = Object.keys(colIdToNameFieldId).length > 0;
    if (hasPeopleColumns) {
      userLookup = buildUserLookup_(rowsToRender);
    }

    rowsToRender.forEach(function (item) {
      dataResponse.addRow(buildRow_(item, fieldsSubset, colIdToFieldId, colIdToType,
        colIdToUrlFieldId, colIdToValueFieldId, colIdToNameFieldId, colIdToEmailFieldId,
        userLookup, colIdToStartFieldId, colIdToEndFieldId));
    });

    return dataResponse.build();

  } catch (e) {
    Logger.log('[Monday Connector] getData exception: ' + e.message);
    if (e.stack) Logger.log('[Monday Connector] Stack: ' + e.stack);
    throwConnectorError_(e.message);
  }
}

/**
 * Transforms a single Monday.com item into a Looker Studio data row.
 * Returns a plain Array of values in the exact order of requestedFields.
 * addRow() requires a plain Array — not a { values: [...] } object.
 *
 * @param  {Object} item             Monday.com item from GraphQL.
 * @param  {Fields} requestedFields  The Looker Studio fields subset.
 * @param  {Object} colIdToFieldId       Monday column ID → Looker field ID.
 * @param  {Object} colIdToType          Monday column ID → Monday type string.
 * @param  {Object} colIdToUrlFieldId    Monday column ID → _url field ID (link/file columns).
 * @param  {Object} colIdToValueFieldId  Monday column ID → _value field ID (link/file columns).
 * @param  {Object} colIdToNameFieldId   Monday column ID → _name field ID (people columns).
 * @param  {Object} colIdToEmailFieldId  Monday column ID → _email field ID (people columns).
 * @param  {Object} userLookup           User ID → { name, email } map.
 * @param  {Object} colIdToStartFieldId  Monday column ID → _start field ID (timeline columns).
 * @param  {Object} colIdToEndFieldId    Monday column ID → _end field ID (timeline columns).
 * @returns {Array}
 */
function buildRow_(item, requestedFields, colIdToFieldId, colIdToType,
    colIdToUrlFieldId, colIdToValueFieldId, colIdToNameFieldId, colIdToEmailFieldId,
    userLookup, colIdToStartFieldId, colIdToEndFieldId) {
  colIdToUrlFieldId   = colIdToUrlFieldId   || {};
  colIdToValueFieldId = colIdToValueFieldId || {};
  colIdToNameFieldId  = colIdToNameFieldId  || {};
  colIdToEmailFieldId = colIdToEmailFieldId || {};
  userLookup          = userLookup          || {};
  colIdToStartFieldId = colIdToStartFieldId || {};
  colIdToEndFieldId   = colIdToEndFieldId   || {};

  var valueMap = {
    item_id      : String(item.id || ''),
    item_name    : item.name || '',
    group_name   : (item.group && item.group.title) ? item.group.title : '',
    record_count : 1
  };

  (item.column_values || []).forEach(function (cv) {
    var fid = colIdToFieldId[cv.id];
    if (fid !== undefined) {
      valueMap[fid] = extractColumnValue_(cv, colIdToType[cv.id]);
    }
    // Populate _url and _value fields for link and file columns
    if (colIdToUrlFieldId[cv.id] !== undefined) {
      var parsed = (cv.type === 'file')
        ? parseFileValue_(cv)
        : parseLinkValue_(cv);
      valueMap[colIdToUrlFieldId[cv.id]]   = parsed.url;
      valueMap[colIdToValueFieldId[cv.id]] = parsed.label;
    }
    // Populate _name and _email fields for people columns
    if (colIdToNameFieldId[cv.id] !== undefined) {
      var people = parsePeopleValue_(cv, userLookup);
      valueMap[colIdToNameFieldId[cv.id]]  = people.names;
      valueMap[colIdToEmailFieldId[cv.id]] = people.emails;
    }
    // Populate _start and _end fields for timeline columns
    if (colIdToStartFieldId[cv.id] !== undefined) {
      var tl = parseTimelineValue_(cv);
      valueMap[colIdToStartFieldId[cv.id]] = tl.start;
      valueMap[colIdToEndFieldId[cv.id]]   = tl.end;
    }
  });

  return requestedFields.asArray().map(function (field) {
    var raw = valueMap[field.getId()];
    if (raw === undefined || raw === null) return '';
    // Preserve native boolean and number types — Looker Studio requires these
    // for BOOLEAN and NUMBER fields. Only stringify TEXT/URL/DATE values.
    if (typeof raw === 'boolean' || typeof raw === 'number') return raw;
    return String(raw);
  });
}


// =============================================================================
// SECTION 4b – SUBITEMS SCHEMA & DATA
// =============================================================================

/**
 * Fetches subitem column definitions for a board.
 * Subitem columns are independent from parent item columns and must be
 * queried separately via the subitems field on the board's items.
 * Results are cached to avoid redundant API calls.
 *
 * @param  {string} boardId  Monday.com board ID.
 * @returns {Array}          Array of { id, title, type } subitem column objects.
 */
function fetchSubitemColumns_(boardId) {
  // Cache subitem columns to avoid redundant API calls within the same session
  var cache     = CacheService.getScriptCache();
  var cacheKey  = 'subitem_cols_' + boardId;
  var cached    = cache.get(cacheKey);
  if (cached) {
    try { return JSON.parse(cached); } catch (e) {}
  }

  var key   = getApiKey_();
  var query = [
    'query {',
    '  boards(ids: [' + boardId + ']) {',
    '    items_page(limit: 1) {',
    '      items {',
    '        subitems {',
    '          board { columns { id title type } }',
    '        }',
    '      }',
    '    }',
    '  }',
    '}'
  ].join('\n');

  var result = callMondayApi_(key, query);
  try {
    var items = result.data.boards[0].items_page.items;
    if (!items || !items.length) return [];
    var subitems = items[0].subitems;
    if (!subitems || !subitems.length) return [];
    var cols = subitems[0].board.columns || [];
    try { cache.put(cacheKey, JSON.stringify(cols), COLUMN_CACHE_TTL); } catch (e) {}
    return cols;
  } catch (e) {
    Logger.log('[Monday Connector] fetchSubitemColumns_ error: ' + e.message);
    return [];
  }
}

/**
 * Builds the Looker Studio Fields schema for the Subitems data type.
 * Dynamically discovers subitem board columns, exactly as buildFields_ does
 * for parent items. Prepends fixed parent context fields so the data source
 * can be blended with the Items data source on parent_item_id.
 *
 * @param  {Object} request  Looker Studio schema request.
 * @returns {Fields}
 */
function buildSubitemsFields_(request) {
  var cc      = DataStudioApp.createCommunityConnector();
  var fields  = cc.getFields();
  var types   = cc.FieldType;
  var agg     = cc.AggregationType;
  var boardId = request.configParams && request.configParams.boardId
    ? request.configParams.boardId
    : null;

  // ── Fixed parent context fields ───────────────────────────────────────────
  fields.newDimension().setId('parent_item_id')
    .setName('Parent Item ID').setType(types.TEXT)
    .setDescription('ID of the parent Monday.com item.');
  fields.newDimension().setId('parent_item_name')
    .setName('Parent Item Name').setType(types.TEXT)
    .setDescription('Name of the parent Monday.com item.');
  fields.newDimension().setId('parent_group_name')
    .setName('Parent Group Name').setType(types.TEXT)
    .setDescription('Group the parent item belongs to.');
  fields.newDimension().setId('subitem_id')
    .setName('Subitem ID').setType(types.TEXT)
    .setDescription('Unique identifier of the subitem.');
  fields.newDimension().setId('subitem_name')
    .setName('Subitem Name').setType(types.TEXT)
    .setDescription('Display name / title of the subitem.');
  fields.newMetric().setId('record_count')
    .setName('Record Count').setType(types.NUMBER)
    .setAggregation(agg.SUM)
    .setDescription('Count of subitems.');

  if (!boardId) return fields;

  // ── Dynamic subitem columns ───────────────────────────────────────────────
  var columns      = fetchSubitemColumns_(boardId);
  var registeredIds = {
    parent_item_id: true, parent_item_name: true, parent_group_name: true,
    subitem_id: true, subitem_name: true, record_count: true
  };

  var NUMERIC_TYPES = { numbers: true, numeric: true, rating: true, vote: true, progress: true };

  columns.forEach(function(col) {
    if (SKIP_COLUMN_TYPES[col.type]) return;

    var fieldId     = deduplicateFieldId_(sanitiseFieldId_(col.id), registeredIds);
    var displayName = sanitiseDisplayName_(col.title);
    registeredIds[fieldId] = true;

    if (col.type === 'link' || col.type === 'file') {
      var urlFieldId   = deduplicateFieldId_(fieldId + '_url',   registeredIds);
      var valueFieldId = deduplicateFieldId_(fieldId + '_value', registeredIds);
      registeredIds[urlFieldId]   = true;
      registeredIds[valueFieldId] = true;
      fields.newDimension().setId(urlFieldId)
        .setName(displayName + '_URL').setType(types.URL)
        .setDescription('URL for the ' + displayName + ' column.');
      fields.newDimension().setId(valueFieldId)
        .setName(displayName + '_Value').setType(types.TEXT)
        .setDescription('Display name / filename for the ' + displayName + ' column.');
      return;
    }

    if (col.type === 'people') {
      var nameFieldId  = deduplicateFieldId_(fieldId + '_name',  registeredIds);
      var emailFieldId = deduplicateFieldId_(fieldId + '_email', registeredIds);
      registeredIds[nameFieldId]  = true;
      registeredIds[emailFieldId] = true;
      fields.newDimension().setId(nameFieldId)
        .setName(displayName + '_Name').setType(types.TEXT)
        .setDescription('Comma-separated names for the ' + displayName + ' people column.');
      fields.newDimension().setId(emailFieldId)
        .setName(displayName + '_Email').setType(types.TEXT)
        .setDescription('Comma-separated emails for the ' + displayName + ' people column.');
      return;
    }

    if (col.type === 'timeline') {
      var startFieldId = deduplicateFieldId_(fieldId + '_start', registeredIds);
      var endFieldId   = deduplicateFieldId_(fieldId + '_end',   registeredIds);
      registeredIds[startFieldId] = true;
      registeredIds[endFieldId]   = true;
      fields.newDimension().setId(startFieldId)
        .setName(displayName + '_Start').setType(types.YEAR_MONTH_DAY)
        .setDescription('Start date for the ' + displayName + ' timeline column.');
      fields.newDimension().setId(endFieldId)
        .setName(displayName + '_End').setType(types.YEAR_MONTH_DAY)
        .setDescription('End date for the ' + displayName + ' timeline column.');
      return;
    }

    if (col.type === 'date') {
      fields.newDimension().setId(fieldId).setName(displayName)
        .setType(types.YEAR_MONTH_DAY)
        .setDescription('Monday.com column type: ' + col.type);
    } else if (col.type === 'checkbox') {
      fields.newDimension().setId(fieldId).setName(displayName)
        .setType(types.BOOLEAN)
        .setDescription('Monday.com column type: ' + col.type);
    } else if (NUMERIC_TYPES[col.type]) {
      fields.newMetric().setId(fieldId).setName(displayName)
        .setType(types.NUMBER)
        .setDescription('Monday.com column type: ' + col.type);
    } else {
      fields.newDimension().setId(fieldId).setName(displayName)
        .setType(types.TEXT)
        .setDescription('Monday.com column type: ' + col.type);
    }
  });

  return fields;
}

/**
 * Fetches all subitems for a board and returns them to Looker Studio.
 * Paginates through parent items and flattens their subitems into one row
 * each, with parent item context repeated on every row.
 *
 * @param  {Object} request  Looker Studio data request.
 * @param  {Object} cc       Community connector instance.
 * @param  {string} boardId  Monday.com board ID.
 * @returns {GetDataResponse}
 */
function getSubitemsData_(request, cc, boardId) {
  var allFields    = buildSubitemsFields_(request);
  var requestedIds = (request.fields && request.fields.length > 0)
    ? request.fields.map(function(f) { return f.name; })
    : allFields.asArray().map(function(f) { return f.getId(); });

  var fieldsSubset = allFields.forIds(requestedIds);

  // Build column lookup maps (same pattern as getData for items)
  var columns      = fetchSubitemColumns_(boardId);
  var registeredIds = {
    parent_item_id: true, parent_item_name: true, parent_group_name: true,
    subitem_id: true, subitem_name: true, record_count: true
  };
  var colIdToFieldId      = {};
  var colIdToType         = {};
  var colIdToUrlFieldId   = {};
  var colIdToValueFieldId = {};
  var colIdToNameFieldId  = {};
  var colIdToEmailFieldId = {};
  var colIdToStartFieldId = {};
  var colIdToEndFieldId   = {};
  var NUMERIC_TYPES = { numbers: true, numeric: true, rating: true, vote: true, progress: true };

  columns.forEach(function(col) {
    if (SKIP_COLUMN_TYPES[col.type]) return;
    var fid = deduplicateFieldId_(sanitiseFieldId_(col.id), registeredIds);
    registeredIds[fid]     = true;
    colIdToFieldId[col.id] = fid;
    colIdToType[col.id]    = col.type;
    if (col.type === 'link' || col.type === 'file') {
      colIdToUrlFieldId[col.id]   = deduplicateFieldId_(fid + '_url',   registeredIds);
      colIdToValueFieldId[col.id] = deduplicateFieldId_(fid + '_value', registeredIds);
      registeredIds[colIdToUrlFieldId[col.id]]   = true;
      registeredIds[colIdToValueFieldId[col.id]] = true;
    }
    if (col.type === 'people') {
      colIdToNameFieldId[col.id]  = deduplicateFieldId_(fid + '_name',  registeredIds);
      colIdToEmailFieldId[col.id] = deduplicateFieldId_(fid + '_email', registeredIds);
      registeredIds[colIdToNameFieldId[col.id]]  = true;
      registeredIds[colIdToEmailFieldId[col.id]] = true;
    }
    if (col.type === 'timeline') {
      colIdToStartFieldId[col.id] = deduplicateFieldId_(fid + '_start', registeredIds);
      colIdToEndFieldId[col.id]   = deduplicateFieldId_(fid + '_end',   registeredIds);
      registeredIds[colIdToStartFieldId[col.id]] = true;
      registeredIds[colIdToEndFieldId[col.id]]   = true;
    }
  });

  var subitems     = fetchAllSubitems_(boardId);
  var dataResponse = cc.newGetDataResponse();
  dataResponse.setFields(fieldsSubset);

  Logger.log('[Monday Connector] Subitems data: ' + subitems.length + ' rows to return.');

  // Build user lookup for people columns if needed
  var subUserLookup = {};
  if (Object.keys(colIdToNameFieldId).length > 0) {
    subUserLookup = buildUserLookup_(subitems);
  }

  subitems.forEach(function(subitem) {
    var valueMap = {
      parent_item_id   : subitem.parent_item_id,
      parent_item_name : subitem.parent_item_name,
      parent_group_name: subitem.parent_group_name,
      subitem_id       : String(subitem.id || ''),
      subitem_name     : subitem.name || '',
      record_count     : 1
    };

    (subitem.column_values || []).forEach(function(cv) {
      var fid = colIdToFieldId[cv.id];
      if (fid !== undefined) {
        valueMap[fid] = extractColumnValue_(cv, colIdToType[cv.id]);
      }
      if (colIdToUrlFieldId[cv.id] !== undefined) {
        var parsed = (cv.type === 'file') ? parseFileValue_(cv) : parseLinkValue_(cv);
        valueMap[colIdToUrlFieldId[cv.id]]   = parsed.url;
        valueMap[colIdToValueFieldId[cv.id]] = parsed.label;
      }
      if (colIdToNameFieldId[cv.id] !== undefined) {
        var people = parsePeopleValue_(cv, subUserLookup);
        valueMap[colIdToNameFieldId[cv.id]]  = people.names;
        valueMap[colIdToEmailFieldId[cv.id]] = people.emails;
      }
      if (colIdToStartFieldId[cv.id] !== undefined) {
        var tl = parseTimelineValue_(cv);
        valueMap[colIdToStartFieldId[cv.id]] = tl.start;
        valueMap[colIdToEndFieldId[cv.id]]   = tl.end;
      }
    });

    var row = fieldsSubset.asArray().map(function(field) {
      var val = valueMap[field.getId()];
      if (val === undefined || val === null) return '';
      if (typeof val === 'boolean' || typeof val === 'number') return val;
      return String(val);
    });
    dataResponse.addRow(row);
  });

  return dataResponse.build();
}

/**
 * Paginates through all items on a board fetching their subitems.
 * Returns a flat array of subitem objects, each with parent item context
 * fields injected so rows can be blended with the Items data source.
 *
 * @param  {string} boardId  Monday.com board ID.
 * @returns {Array}          Flat array of subitem objects.
 */
function fetchAllSubitems_(boardId) {
  var key    = getApiKey_();
  var rows   = [];
  var cursor = null;
  var page   = 0;

  do {
    page++;
    var paginationArg = cursor
      ? 'limit: ' + PAGE_LIMIT + ', cursor: "' + cursor + '"'
      : 'limit: ' + PAGE_LIMIT;

    var query = [
      'query {',
      '  boards(ids: [' + boardId + ']) {',
      '    items_page(' + paginationArg + ') {',
      '      cursor',
      '      items {',
      '        id name',
      '        group { title }',
      '        subitems {',
      '          id name',
      '          column_values { id type text value }',
      '        }',
      '      }',
      '    }',
      '  }',
      '}'
    ].join('\n');

    var result    = callMondayApi_(key, query);
    var itemsPage = result &&
                    result.data &&
                    result.data.boards &&
                    result.data.boards[0] &&
                    result.data.boards[0].items_page;

    if (!itemsPage || !itemsPage.items || !itemsPage.items.length) break;

    itemsPage.items.forEach(function(item) {
      var groupName = (item.group && item.group.title) ? item.group.title : '';
      (item.subitems || []).forEach(function(subitem) {
        subitem.parent_item_id    = String(item.id || '');
        subitem.parent_item_name  = item.name || '';
        subitem.parent_group_name = groupName;
        rows.push(subitem);
      });
    });

    cursor = itemsPage.cursor || null;
    Logger.log('[Monday Connector] Subitems page ' + page +
      ' fetched. Rows so far: ' + rows.length +
      '. More pages: ' + (cursor ? 'yes' : 'no'));

  } while (cursor);

  Logger.log('[Monday Connector] Subitems fetch complete. Total rows: ' + rows.length);

  // Inject positional auto_number values per group, same logic as parent items
  rows = injectAutoNumberValues_(rows, boardId);

  return rows;
}


// =============================================================================
// SECTION 4b – UPDATES (COMMENTS) SCHEMA & DATA
// =============================================================================

/**
 * Builds the Looker Studio Fields schema for the Updates (comments) data type.
 * Returns a flat table with one row per comment/update per item.
 *
 * Fields returned:
 *   item_id        – ID of the parent item
 *   item_name      – Name of the parent item
 *   group_name     – Group the parent item belongs to
 *   update_id      – Unique ID of the update/comment
 *   update_body    – The comment text (HTML tags stripped)
 *   update_author  – Display name of the person who posted the comment
 *   update_created – Date the comment was posted (YEAR_MONTH_DAY)
 *   record_count   – Always 1, for counting in Looker Studio
 *
 * @returns {Fields}
 */
function buildUpdatesFields_() {
  var cc     = DataStudioApp.createCommunityConnector();
  var fields = cc.getFields();
  var types  = cc.FieldType;

  fields.newDimension().setId('item_id')
    .setName('Item ID').setType(types.TEXT)
    .setDescription('ID of the parent Monday.com item.');
  fields.newDimension().setId('item_name')
    .setName('Item Name').setType(types.TEXT)
    .setDescription('Name of the parent Monday.com item.');
  fields.newDimension().setId('group_name')
    .setName('Group Name').setType(types.TEXT)
    .setDescription('Group the parent item belongs to.');
  fields.newDimension().setId('source')
    .setName('Source').setType(types.TEXT)
    .setDescription('Whether the comment was posted on an Item or a Subitem.');
  fields.newDimension().setId('source_id')
    .setName('Source ID').setType(types.TEXT)
    .setDescription('ID of the item or subitem the comment belongs to.');
  fields.newDimension().setId('source_name')
    .setName('Source Name').setType(types.TEXT)
    .setDescription('Name of the item or subitem the comment belongs to.');
  fields.newDimension().setId('update_id')
    .setName('Update ID').setType(types.TEXT)
    .setDescription('Unique identifier of the comment/update.');
  fields.newDimension().setId('update_body')
    .setName('Comment').setType(types.TEXT)
    .setDescription('Text content of the comment (HTML stripped).');
  fields.newDimension().setId('update_author')
    .setName('Author').setType(types.TEXT)
    .setDescription('Display name of the person who posted the comment.');
  fields.newDimension().setId('update_author_email')
    .setName('Author Email').setType(types.TEXT)
    .setDescription('Email address of the person who posted the comment.');
  fields.newDimension().setId('update_created')
    .setName('Posted Date').setType(types.YEAR_MONTH_DAY_SECOND)
    .setDescription('Date and time the comment was posted.');
  fields.newMetric().setId('record_count')
    .setName('Record Count').setType(types.NUMBER)
    .setDescription('Count of comments.');

  return fields;
}

/**
 * Fetches all updates (comments) for a board and returns them to Looker Studio.
 * Each update becomes one row in the flat table, with parent item details
 * repeated on each row so the data can be blended with the Items data source.
 *
 * @param  {Object} request  Looker Studio data request.
 * @param  {Object} cc       Community connector instance.
 * @param  {string} boardId  Monday.com board ID.
 * @returns {GetDataResponse}
 */
function getUpdatesData_(request, cc, boardId) {
  var allFields    = buildUpdatesFields_();
  var requestedIds = (request.fields && request.fields.length > 0)
    ? request.fields.map(function(f) { return f.name; })
    : allFields.asArray().map(function(f) { return f.getId(); });

  var fieldsSubset = allFields.forIds(requestedIds);
  var updates      = fetchAllUpdates_(boardId);

  Logger.log('[Monday Connector] Updates data: ' + updates.length + ' rows to return.');

  var dataResponse = cc.newGetDataResponse();
  dataResponse.setFields(fieldsSubset);

  updates.forEach(function(update) {
    var row = fieldsSubset.asArray().map(function(field) {
      var val = update[field.getId()];
      return (val !== undefined && val !== null) ? val : '';
    });
    dataResponse.addRow(row);
  });

  return dataResponse.build();
}

/**
 * Fetches all updates (comments) for all items AND subitems on a board.
 * Paginates through items, collecting updates from both the item itself
 * and any subitems it has. Returns a flat array with a 'source' field
 * indicating whether each comment came from an "Item" or "Subitem".
 *
 * @param  {string} boardId  Monday.com board ID.
 * @returns {Array}          Flat array of update row objects.
 */
function fetchAllUpdates_(boardId) {
  var key    = getApiKey_();
  var rows   = [];
  var cursor = null;
  var page   = 0;

  do {
    page++;
    var paginationArg = cursor
      ? 'limit: 100, cursor: "' + cursor + '"'
      : 'limit: 100';

    var query = [
      'query {',
      '  boards(ids: [' + boardId + ']) {',
      '    items_page(' + paginationArg + ') {',
      '      cursor',
      '      items {',
      '        id name',
      '        group { title }',
      '        updates(limit: 100) {',
      '          id body created_at',
      '          creator { name email }',
      '        }',
      '        subitems {',
      '          id name',
      '          updates(limit: 100) {',
      '            id body created_at',
      '            creator { name email }',
      '          }',
      '        }',
      '      }',
      '    }',
      '  }',
      '}'
    ].join('\n');

    var result    = callMondayApi_(key, query);
    var itemsPage = result &&
                    result.data &&
                    result.data.boards &&
                    result.data.boards[0] &&
                    result.data.boards[0].items_page;

    if (!itemsPage || !itemsPage.items || !itemsPage.items.length) break;

    itemsPage.items.forEach(function(item) {
      var groupName = (item.group && item.group.title) ? item.group.title : '';

      // Parent item updates
      (item.updates || []).forEach(function(update) {
        rows.push(buildUpdateRow_(update, item, groupName, 'Item', item));
      });

      // Subitem updates
      (item.subitems || []).forEach(function(subitem) {
        (subitem.updates || []).forEach(function(update) {
          rows.push(buildUpdateRow_(update, item, groupName, 'Subitem', subitem));
        });
      });
    });

    cursor = itemsPage.cursor || null;
    Logger.log('[Monday Connector] Updates page ' + page +
      ' fetched. Rows so far: ' + rows.length +
      '. More pages: ' + (cursor ? 'yes' : 'no'));

  } while (cursor);

  Logger.log('[Monday Connector] Updates fetch complete. Total rows: ' + rows.length);
  return rows;
}

/**
 * Builds a single update row object from a Monday.com update node.
 *
 * @param  {Object} update     The update/comment node.
 * @param  {Object} item       The parent item node.
 * @param  {string} groupName  The parent item's group name.
 * @param  {string} source     'Item' or 'Subitem'.
 * @param  {Object} sourceNode The item or subitem the comment belongs to.
 * @returns {Object}           Flat row object for Looker Studio.
 */
function buildUpdateRow_(update, item, groupName, source, sourceNode) {
  return {
    item_id             : String(item.id || ''),
    item_name           : item.name || '',
    group_name          : groupName,
    source              : source,
    source_id           : String(sourceNode.id || ''),
    source_name         : sourceNode.name || '',
    update_id           : String(update.id || ''),
    update_body         : stripHtml_(update.body || ''),
    update_author       : (update.creator && update.creator.name)
                          ? update.creator.name  : '',
    update_author_email : (update.creator && update.creator.email)
                          ? update.creator.email : '',
    update_created      : formatDateYMD_(update.created_at),
    record_count        : 1
  };
}

/**
 * Strips HTML tags from a Monday.com update body string.
/**
 * Strips HTML tags from a Monday.com update body string.
 * Monday stores update text as HTML; we return plain text for Looker Studio.
 *
 * @param  {string} html  HTML string from Monday update body.
 * @returns {string}      Plain text with tags removed.
 */
function stripHtml_(html) {
  if (!html) return '';
  return html
    .replace(/<[^>]+>/g, ' ')  // Replace tags with space
    .replace(/&nbsp;/g, ' ')
    .replace(/&amp;/g,  '&')
    .replace(/&lt;/g,   '<')
    .replace(/&gt;/g,   '>')
    .replace(/&quot;/g, '"')
    .replace(/\s+/g,    ' ')   // Collapse whitespace
    .trim();
}

/**
 * Converts a Monday.com ISO timestamp (e.g. "2024-03-15T10:30:00Z") to
 * YYYYMMDDHHMMSS format required by Looker Studio's YEAR_MONTH_DAY_SECOND type.
 *
 * @param  {string} isoString  ISO 8601 timestamp string.
 * @returns {string}           Datetime in YYYYMMDDHHMMSS format, or '' if invalid.
 */
function formatDateYMD_(isoString) {
  if (!isoString) return '';
  try {
    // Parse the ISO string and extract components
    // e.g. "2024-03-15T10:30:45.000Z" → "20240315103045"
    var dt = new Date(isoString);
    if (isNaN(dt.getTime())) return '';

    var yyyy = dt.getUTCFullYear();
    var mm   = String(dt.getUTCMonth() + 1).padStart(2, '0');
    var dd   = String(dt.getUTCDate()).padStart(2, '0');
    var hh   = String(dt.getUTCHours()).padStart(2, '0');
    var min  = String(dt.getUTCMinutes()).padStart(2, '0');
    var ss   = String(dt.getUTCSeconds()).padStart(2, '0');

    return yyyy + mm + dd + hh + min + ss;
  } catch (e) {
    return '';
  }
}


// =============================================================================
// SECTION 5 – MONDAY.COM API LAYER
// =============================================================================

/**
 * Returns column metadata for a board, served from CacheService when possible.
 *
 * @param  {string} boardId
 * @returns {Array}  Array of { id, title, type } objects.
 */
function fetchBoardColumns_(boardId) {
  var cache    = CacheService.getUserCache();
  var cacheKey = 'monday_cols_' + boardId;
  var cached   = cache.get(cacheKey);

  if (cached) {
    try { return JSON.parse(cached); } catch (e) { /* cache corrupt, re-fetch */ }
  }

  var query = [
    'query {',
    '  boards(ids: [' + boardId + ']) {',
    '    columns { id title type }',
    '  }',
    '}'
  ].join(' ');

  var result = callMondayApi_(getApiKey_(), query);

  if (!result || !result.data || !result.data.boards || !result.data.boards.length) {
    throwConnectorError_(
      'Board not found or access denied. ' +
      'Verify Board ID "' + boardId + '" and that your API key has View access.'
    );
  }

  var columns = result.data.boards[0].columns || [];

  try { cache.put(cacheKey, JSON.stringify(columns), COLUMN_CACHE_TTL); }
  catch (e) { Logger.log('[Monday Connector] Column cache write failed: ' + e.message); }

  return columns;
}

/**
 * Fetches a small sample of items (one page of 10) for sampleExtraction
 * requests from Looker Studio, avoiding a full board paginated fetch.
 *
 * @param  {string} boardId  Monday.com board ID.
 * @returns {Array}          Array of up to 10 item objects.
 */
function fetchSampleItems_(boardId) {
  var key   = getApiKey_();
  var query = [
    'query {',
    '  boards(ids: [' + boardId + ']) {',
    '    items_page(limit: 10) {',
    '      items {',
    '        id name',
    '        group { title }',
    '        column_values { id type text value }',
    '      }',
    '    }',
    '  }',
    '}'
  ].join('\n');

  var result = callMondayApi_(key, query);
  try {
    return result.data.boards[0].items_page.items || [];
  } catch (e) {
    return [];
  }
}


/**
 * Fetches ALL items from a board using cursor-based pagination.
 * Only the fields required for row assembly are requested to minimise
 * GraphQL query complexity and response payload size.
 *
 * @param  {string} boardId
 * @returns {Array}  Flat array of all item objects.
 */
function fetchAllItems_(boardId) {
  var key    = getApiKey_();
  var items  = [];
  var cursor = null;

  do {
    var paginationArg = cursor
      ? 'limit: ' + PAGE_LIMIT + ', cursor: "' + cursor + '"'
      : 'limit: ' + PAGE_LIMIT;

    var query = [
      'query {',
      '  boards(ids: [' + boardId + ']) {',
      '    items_page(' + paginationArg + ') {',
      '      cursor',
      '      items {',
      '        id name',
      '        group { title }',
      '        column_values {',
      '          id type text value',
      '        }',
      '      }',
      '    }',
      '  }',
      '}'
    ].join('\n');

    var result    = callMondayApi_(key, query);
    var itemsPage = result &&
                    result.data &&
                    result.data.boards &&
                    result.data.boards[0] &&
                    result.data.boards[0].items_page;

    if (!itemsPage || !itemsPage.items || !itemsPage.items.length) break;

    items  = items.concat(itemsPage.items);
    cursor = itemsPage.cursor || null;

    Logger.log('[Monday Connector] Page fetched. Items so far: ' + items.length +
      '. More pages: ' + (cursor ? 'yes' : 'no'));

  } while (cursor);

  Logger.log('[Monday Connector] Pagination complete. Total items: ' + items.length);

  // auto_number columns are not returned by Monday's API through any endpoint.
  // Since auto_number is simply a sequential integer assigned in board order,
  // and Monday returns items in board order, we inject the value ourselves
  // by using the item's 1-based position in the results array.
  items = injectAutoNumberValues_(items, boardId);

  return items;
}

/**
 * Injects auto_number column values into items based on their position in the
 * results array. Monday's API does not expose auto_number values through any
 * column_values query, but auto_number is always a sequential integer in board
 * order, which matches the order items are returned by the API.
 *
 * @param  {Array}  items    Items array from the paginated fetch.
 * @param  {string} boardId  Monday.com board ID.
 * @returns {Array}          Items with auto_number column_values injected.
 */
function injectAutoNumberValues_(items, boardId) {
  var columns       = fetchBoardColumns_(boardId);
  var autoNumberIds = columns
    .filter(function(col) { return col.type === 'auto_number'; })
    .map(function(col) { return col.id; });

  if (!autoNumberIds.length) return items;

  Logger.log('[Monday Connector] Injecting positional auto_number values for columns: ' +
    autoNumberIds.join(', '));

  // auto_number resets to 1 at the start of each group in Monday.com,
  // so we track a counter per group title rather than using overall position.
  var groupCounters = {};

  items.forEach(function(item) {
    var groupTitle = (item.group && item.group.title) ? item.group.title : '__default__';

    if (groupCounters[groupTitle] === undefined) {
      groupCounters[groupTitle] = 0;
    }
    groupCounters[groupTitle]++;
    var positionInGroup = groupCounters[groupTitle];

    autoNumberIds.forEach(function(colId) {
      var existing = (item.column_values || []);
      var found = false;
      for (var i = 0; i < existing.length; i++) {
        if (existing[i].id === colId) {
          existing[i].text  = String(positionInGroup);
          existing[i].value = String(positionInGroup);
          found = true;
          break;
        }
      }
      if (!found) {
        existing.push({
          id   : colId,
          type : 'auto_number',
          text : String(positionInGroup),
          value: String(positionInGroup)
        });
        item.column_values = existing;
      }
    });
  });

  return items;
}


/**
 * Executes a GraphQL query against the Monday.com API.
 *
 * SECURITY: The API key travels only in the HTTPS Authorization header.
 * It is never interpolated into the query string or written to any log.
 *
 * @param  {string} apiKey  Monday.com API token.
 * @param  {string} query   GraphQL query string.
 * @returns {Object}        Parsed JSON response.
 */
function callMondayApi_(apiKey, query) {
  var options = {
    method           : 'post',
    contentType      : 'application/json',
    headers          : {
      'Authorization': 'Bearer ' + apiKey,
      'API-Version'  : MONDAY_API_VERSION
    },
    payload          : JSON.stringify({ query: query }),
    muteHttpExceptions: true
  };

  var response     = UrlFetchApp.fetch(MONDAY_API_URL, options);
  var responseCode = response.getResponseCode();

  Logger.log('[Monday Connector] API response HTTP ' + responseCode);

  if (responseCode !== 200) {
    throwConnectorError_(
      'Monday.com API returned HTTP ' + responseCode + '. ' +
      'Check your API key and that the Monday.com service is reachable.'
    );
  }

  var parsed;
  try {
    parsed = JSON.parse(response.getContentText());
  } catch (e) {
    throwConnectorError_('Could not parse Monday.com API response: ' + e.message);
  }

  if (parsed.errors && parsed.errors.length) {
    var msgs = parsed.errors.map(function (e) { return e.message || 'Unknown error'; }).join('; ');
    Logger.log('[Monday Connector] GraphQL errors: ' + msgs);
    throwConnectorError_('Monday.com GraphQL error: ' + msgs);
  }

  return parsed;
}

/**
 * Retrieves the stored API key from UserProperties.
 * Never writes the key to any log.
 *
 * @returns {string}
 */
function getApiKey_() {
  var key = PropertiesService.getUserProperties().getProperty(API_KEY_PROPERTY);
  if (!key) {
    throwConnectorError_(
      'No API key found. Please disconnect and reconnect the data source to re-authenticate.'
    );
  }
  return key;
}


// =============================================================================
// SECTION 6 – VALUE EXTRACTION
// =============================================================================

/**
 * Extracts a scalar string value from a Monday.com column_value node.
 *
 * For most types the pre-formatted `text` field Monday.com provides is used
 * directly. For types where `text` is absent or less useful (e.g. checkbox),
 * the raw `value` JSON is parsed instead.
 *
 * All return values are cast to String so that buildRow_ can safely call
 * String(raw) without risk of '[object Object]' or similar artefacts.
 *
 * @param  {Object} cv          column_values node { id, type, text, value }.
 * @param  {string} columnType  Monday.com type string for this column.
 * @returns {string}
 */
function extractColumnValue_(cv, columnType) {
  if (!cv) return '';

  var rawText  = (cv.text  !== null && cv.text  !== undefined) ? String(cv.text)  : '';
  var rawValue = (cv.value !== null && cv.value !== undefined) ? cv.value         : null;
  var colType  = cv.type || columnType || '';

  try {
    switch (colType) {

      // Checkbox — return actual boolean (BOOLEAN type field)
      case 'checkbox': {
        if (!rawValue) return false;
        try {
          var cb = JSON.parse(rawValue);
          return cb.checked === true;
        } catch (e) {
          return false;
        }
      }

      // Date — YEAR_MONTH_DAY type requires YYYYMMDD format (no hyphens).
      case 'date': {
        if (!rawValue) return '';
        try {
          var d = JSON.parse(rawValue);
          if (!d.date) return '';
          // Strip hyphens: '2024-03-15' → '20240315'
          return d.date.replace(/-/g, '');
        } catch (e) {
          return '';
        }
      }

      // Numeric columns — return a JavaScript number for NUMBER Metric fields.
      // Falls back to 0 if the value is empty or unparseable.
      case 'numbers':
      case 'numeric':
      case 'rating':
      case 'vote':
      case 'progress': {
        var num = parseFloat(rawText);
        return isNaN(num) ? 0 : num;
      }

      // auto_number — Monday's API does not return this column type via any
      // column_values query. Values are injected positionally by injectAutoNumberValues_.
      case 'auto_number': {
        if (rawText) return rawText;
        // Fallback: parse value JSON if text is unexpectedly empty
        if (rawValue) {
          try { return String(JSON.parse(rawValue)); } catch(e) {}
        }
        return '';
      }

      // All other types — use Monday's pre-formatted text value
      default:
        return rawText;
    }
  } catch (e) {
    Logger.log('[Monday Connector] Value extraction warning (' + colType + '): ' + e.message);
    return rawText;
  }
}




/**
 * Builds a user ID → { name, email } lookup map by collecting all person IDs
 * from people column values across all items, then batch-querying Monday's
 * users endpoint to resolve them.
 *
 * @param  {Array}  items  Items array from fetchAllItems_.
 * @returns {Object}       Map of user ID (string) → { name, email }.
 */
function buildUserLookup_(items) {
  // Collect all unique person IDs from people column values
  var personIds = {};
  items.forEach(function(item) {
    (item.column_values || []).forEach(function(cv) {
      if (cv.type !== 'people' || !cv.value) return;
      try {
        var obj = JSON.parse(cv.value);
        (obj.personsAndTeams || []).forEach(function(p) {
          if (p.kind === 'person') personIds[String(p.id)] = true;
        });
      } catch (e) {}
    });
  });

  var ids = Object.keys(personIds);
  if (!ids.length) return {};

  Logger.log('[Monday Connector] Resolving ' + ids.length + ' unique person IDs to emails.');

  var key   = getApiKey_();
  var query = [
    'query {',
    '  users(ids: [' + ids.join(', ') + ']) {',
    '    id name email',
    '  }',
    '}'
  ].join('\n');

  var result = callMondayApi_(key, query);
  var lookup = {};
  try {
    (result.data.users || []).forEach(function(user) {
      lookup[String(user.id)] = { name: user.name || '', email: user.email || '' };
    });
  } catch (e) {
    Logger.log('[Monday Connector] buildUserLookup_ error: ' + e.message);
  }

  Logger.log('[Monday Connector] User lookup built. ' + Object.keys(lookup).length + ' users resolved.');
  return lookup;
}

/**
 * Parses a Monday.com people column_value into comma-separated names and emails.
 * Falls back to the text field for names if the user lookup is incomplete.
 *
 * @param  {Object} cv          column_values node for a people column.
 * @param  {Object} userLookup  User ID → { name, email } map.
 * @returns {Object}            { names: string, emails: string }
 */
function parsePeopleValue_(cv, userLookup) {
  userLookup = userLookup || {};
  var names  = [];
  var emails = [];

  if (cv && cv.value) {
    try {
      var obj = JSON.parse(cv.value);
      (obj.personsAndTeams || []).forEach(function(p) {
        if (p.kind !== 'person') return;
        var user = userLookup[String(p.id)];
        if (user) {
          names.push(user.name);
          emails.push(user.email);
        }
      });
    } catch (e) {}
  }

  // Fallback to text field for names if lookup didn't resolve everyone
  if (!names.length && cv && cv.text) {
    names = String(cv.text).split(', ');
  }

  return {
    names : names.join(', '),
    emails: emails.join(', ')
  };
}


/**
 * Parses a Monday.com file column_value into a URL and filename.
 *
 * Monday.com file columns store an array of file assets in the value JSON.
 * Each asset has a 'name' (filename) and either a 'url' (for external files
 * such as Google Drive links) or a 'public_download_url' (for files uploaded
 * directly to Monday). We return the first file in the array.
 *
 * Example value JSON:
 *   { "files": [{ "name": "myfile.pdf", "url": "https://drive.google.com/..." }] }
 *
 * @param  {Object} cv  column_values node for a file column.
 * @returns {Object}    { url: string, label: string }
 */
function parseFileValue_(cv) {
  if (cv && cv.value) {
    try {
      var obj = JSON.parse(cv.value);
      // Value is either a files array wrapper or a direct array
      var files = obj.files || (Array.isArray(obj) ? obj : null);
      if (files && files.length > 0) {
        var file  = files[0];
        // Monday file columns use 'linkToFile' for the URL, not 'url'.
        // 'public_download_url' is kept as a fallback for other file types.
        var url   = file.linkToFile || file.url || file.public_download_url || '';
        var label = file.name || url;
        return { url: url, label: label };
      }
    } catch (e) {}
  }
  // Fallback: Monday's text field contains the URL directly for file columns
  var fallbackUrl = (cv && cv.text) ? String(cv.text) : '';
  return { url: fallbackUrl, label: fallbackUrl };
}


/**
 * Parses a Monday.com timeline column_value into start and end dates.
 * Returns both dates in YYYYMMDD format for Looker Studio's YEAR_MONTH_DAY type.
 *
 * Example value JSON: {"from":"2025-09-17","to":"2025-09-19","changed_at":"..."}
 *
 * @param  {Object} cv  column_values node for a timeline column.
 * @returns {Object}    { start: string, end: string } in YYYYMMDD format.
 */
function parseTimelineValue_(cv) {
  if (cv && cv.value) {
    try {
      var obj   = JSON.parse(cv.value);
      var start = obj.from ? obj.from.replace(/-/g, '') : '';
      var end   = obj.to   ? obj.to.replace(/-/g, '')   : '';
      return { start: start, end: end };
    } catch (e) {}
  }
  return { start: '', end: '' };
}


/**
 * Parses a Monday.com link column_value into its URL and display label.
 * Used by buildRow_ to populate the _url and _value companion fields.
 *
 * @param  {Object} cv  column_values node for a link column.
 * @returns {Object}    { url: string, label: string }
 */
function parseLinkValue_(cv) {
  if (cv && cv.value) {
    try {
      var obj   = JSON.parse(cv.value);
      var url   = obj.url  || '';
      var label = obj.text || url;
      return { url: url, label: label };
    } catch (e) {}
  }
  // Fallback: Monday's text field is "label - url"
  if (cv && cv.text) {
    var parts = String(cv.text).split(' - ');
    if (parts.length > 1) {
      return {
        url  : parts[parts.length - 1],
        label: parts.slice(0, parts.length - 1).join(' - ')
      };
    }
    return { url: String(cv.text), label: String(cv.text) };
  }
  return { url: '', label: '' };
}


// =============================================================================
// SECTION 7 – FIELD NAME UTILITIES
// =============================================================================

/**
 * Converts a Monday.com column ID into a safe Looker Studio field ID.
 * All IDs are prefixed with 'col_' to prevent clashes with reserved words
 * (e.g. 'name', 'link', 'status', 'type', 'value', 'label', etc.).
 *
 * @param  {string} id  Raw Monday.com column ID.
 * @returns {string}    Sanitised field ID.
 */
function sanitiseFieldId_(id) {
  if (!id) return 'col_unknown';
  var safe = String(id).replace(/[^a-zA-Z0-9_]/g, '_').toLowerCase();
  return 'col_' + safe;
}

/**
 * Appends '_2' to a field ID that is already registered.
 *
 * @param  {string} candidateId    Sanitised field ID to check.
 * @param  {Object} registeredIds  Map of already-registered IDs.
 * @returns {string}               A unique field ID.
 */
function deduplicateFieldId_(candidateId, registeredIds) {
  if (registeredIds[candidateId]) {
    var deduped = candidateId + '_2';
    Logger.log('[Monday Connector] Duplicate field ID "' + candidateId +
      '" → renamed to "' + deduped + '".');
    return deduped;
  }
  return candidateId;
}

/**
 * Sanitises a Monday.com column title for use as a Looker Studio display name.
 * Removes quote characters, replaces unsupported special characters with
 * underscores, and collapses repeated underscores.
 *
 * @param  {string} title  Raw column title.
 * @returns {string}       Sanitised display name.
 */
function sanitiseDisplayName_(title) {
  if (!title || !title.trim()) return 'Unnamed_Field';
  return title
    .trim()
    .replace(/['"''""\u201C\u201D]/g, '')
    .replace(/[^a-zA-Z0-9 _\-().\/]/g, '_')
    .replace(/_+/g, '_')
    .replace(/^_+|_+$/g, '')
    .trim()
    || 'Unnamed_Field';
}


// =============================================================================
// SECTION 8 – ERROR HANDLING & ADMIN
// =============================================================================

/**
 * Throws a Looker Studio UserError with a user-friendly message.
 * Admin users (see isAdminUser) also see the debug text.
 *
 * @param  {string} message
 */
function throwConnectorError_(message) {
  DataStudioApp.createCommunityConnector()
    .newUserError()
    .setDebugText('[Monday Connector] ' + message)
    .setText('An error occurred connecting to Monday.com:\n\n' + message)
    .throwException();
}

/**
 * Returns true to indicate admin-level debug messages should be shown.
 * Appropriate for an internal deployment. For a public connector, restrict
 * this to a whitelist of admin email addresses.
 *
 * @returns {boolean}
 */
function isAdminUser() {
  return true;
}
