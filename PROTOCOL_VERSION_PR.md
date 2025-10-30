# Add Protocol Version Tracking to Projects Schema

## Summary

This PR adds a new `protocol_version` field to the projects schema that captures the version information for carbon offset methodologies/protocols. The field is a parallel array to the existing `protocol` field, where `protocol_version[i]` corresponds to `protocol[i]`.

## Motivation

Carbon offset projects use specific versions of methodologies (protocols), and version information is critical for:
- **Tracking methodology evolution**: Different versions have different requirements and credibility
- **Data analysis**: Researchers need to analyze performance by protocol version
- **Transparency**: Buyers and analysts want to know which methodology version was used
- **Compliance**: Some markets require specific protocol versions

Currently, version information exists in the raw registry data but is lost during harmonization. This PR preserves that information.

## Data Coverage

Based on analysis of the `all-protocol-mapping.json` file:

- **Total known protocol strings**: 878
- **Strings with version info**: 360 (41%)
- **Strings without version info**: 518 (59%)

### Version Coverage by Registry:

| Registry Type | Protocols | Version Coverage |
|---------------|-----------|------------------|
| CDM (ACM/AMS) | ~180 protocols | ~70-80% |
| Verra VM | ~50 protocols | ~5% |
| ACR | ~20 protocols | ~2% |
| ARB/CAR | ~15 protocols | ~0% |

**Note**: Limited coverage for Verra VM, ACR, and ARB protocols reflects what's available in source registry data, not a limitation of this implementation.

## Implementation Details

### Schema Changes

**Added to `offsets_db_data/models.py`:**
```python
'protocol_version': pa.Column(pa.Object, nullable=True)  # Array of strings (parallel to protocol)
```

### Core Functions

**1. `extract_protocol_version_pairs(protocol_string: str)`**
- Extracts protocol name and version pairs from raw registry strings
- Handles multiple protocols separated by: `; & , and`
- Version formats supported: `v1.6`, `Version 19.0`, `ver 21`, `version21.0`, `&20.0`
- Returns: `[(protocol_name, version), ...]`

**2. `extract_protocol_versions(df)`**
- DataFrame method that runs BEFORE protocol harmonization
- Creates temporary `protocol_version_raw` column with {protocol_name: version} mapping
- Preserves version info before protocol names are normalized

**3. `align_protocol_versions(df)`**
- DataFrame method that runs AFTER protocol harmonization
- Matches extracted versions to normalized protocol names
- Creates final `protocol_version` array parallel to `protocol` array
- Handles case-insensitive matching and punctuation differences

### Pipeline Integration

Added to all registry processing pipelines (`vcs.py`, `gld.py`, `apx.py`):

```python
data = (
    df...
    .extract_protocol_versions()        # NEW: Extract before mapping
    .map_protocol(...)                  # Existing: Normalize names
    .align_protocol_versions()          # NEW: Align to normalized names
    ...
)
```

## Examples

### Single Protocol with Version
```
Input:  "ACM0001 Version 19.0"
Output: protocol = ['acm0001']
        protocol_version = ['19.0']
```

### Multiple Protocols with Versions
```
Input:  "ACM0001: Version 19.0; ACM0022: Version 3.0"
Output: protocol = ['acm0001', 'acm0022']
        protocol_version = ['19.0', '3.0']
```

### Protocol without Version
```
Input:  "VM0007 REDD+ Framework"
Output: protocol = ['vm0007']
        protocol_version = [None]
```

### Partial Versions
```
Input:  "ACM0001 v19.0 and ACM0022"
Output: protocol = ['acm0001', 'acm0022']
        protocol_version = ['19.0', None]
```

## Testing

Comprehensive test suite added in `tests/test_protocol_version.py`:

- ✅ Single protocol with/without version
- ✅ Multi-protocol with various separators (`;`, `&`, `and`, `,`)
- ✅ Version format variations (`v1.6`, `Version 19.0`, `ver 21`, etc.)
- ✅ Edge cases (no space, no decimal, empty strings, None values)
- ✅ DataFrame integration tests
- ✅ End-to-end pipeline tests
- ✅ Case-insensitive and punctuation-agnostic matching

All tests passing ✅

## Backward Compatibility

- **Non-breaking change**: New optional field added to schema
- **Existing pipelines**: Continue to work without modification
- **Parquet files**: New field is nullable, fully backward compatible
- **Data consumers**: Can choose to use or ignore the new field

## Data Quality Notes

### Expected Behavior

1. **CDM Methodologies (ACM/AMS)**: Most will have versions
   - Example: "ACM0001 Version 19.0" → `['19.0']`

2. **Verra VM Protocols**: Most will NOT have versions
   - Verra typically reports: "VM0007" (no version)
   - Result: `protocol_version = [None]`

3. **ACR/ARB/CAR**: Rarely include versions in source data
   - Result: `protocol_version = [None]` for most projects

4. **Multi-protocol projects**: May have mixed version availability
   - Example: "ACM0001 v19.0 and VM0007" → `['19.0', None]`

### Why Some Protocols Have No Versions

This reflects **source data limitations**, not implementation gaps:
- Some registries don't report version numbers publicly
- Some protocols don't use version numbers
- Some projects predate version tracking systems

## Files Changed

### Core Implementation
- `offsets_db_data/models.py` - Added `protocol_version` to schema
- `offsets_db_data/projects.py` - Added extraction and alignment functions
- `offsets_db_data/vcs.py` - Updated Verra processing pipeline
- `offsets_db_data/gld.py` - Updated Gold Standard processing pipeline
- `offsets_db_data/apx.py` - Updated ACR/CAR/ART processing pipeline

### Tests
- `tests/test_protocol_version.py` - Comprehensive test suite (new file)

### Documentation
- `PROTOCOL_VERSION_PR.md` - This PR description (new file)

## Future Enhancements

Potential improvements for future PRs:

1. **Extended Pattern Matching**: Add support for more version formats if found in data
2. **Version Validation**: Cross-reference extracted versions against known methodology versions
3. **Version Metadata**: Add protocol effective dates, deprecation status
4. **Manual Overrides**: Config file for manual version corrections
5. **Data Quality Reports**: Generate reports on version coverage by registry

## Migration Notes

For downstream consumers (like `vcm-fyi-api`):

1. **Database Migration**: Add `protocol_version TEXT[]` column to projects table
2. **API Schema**: Add `protocol_version: list[str | None]` to ProjectBase model
3. **Frontend**: Update TypeScript interfaces to include `protocol_version?: (string | null)[]`
4. **Parquet Re-processing**: Reprocess existing parquet files to populate version field

## Checklist

- [x] Schema updated with new field
- [x] Core extraction functions implemented
- [x] All registry pipelines updated
- [x] Comprehensive tests added
- [x] Tests passing
- [x] Documentation written
- [x] Examples provided
- [x] Backward compatibility verified
- [ ] Code review requested
- [ ] CI/CD passing (pending)

## Questions for Reviewers

1. **Field Naming**: Is `protocol_version` clear enough, or should it be `protocol_versions` (plural)?
2. **Null Handling**: Current approach uses `[None]` for missing versions. Alternative: empty array `[]`?
3. **Version Normalization**: Current approach adds `.0` to integers. Keep or remove?
4. **Documentation**: Any additional docs needed (README, changelog, etc.)?

## Related Issues

This addresses the need for protocol version tracking mentioned in discussions about data transparency and methodology analysis.

---

**Ready for review!** 🚀

Please test with your local data pipelines and provide feedback on the implementation.

