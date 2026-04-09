# Target Data Model Draft

This document is the working draft for Task 1.

Status:
- Not finalized
- Do not create DDL from this file until Task 1 is reviewed

## Proposed Tables

### `candidate_profiles_v2`
Purpose:
- One stable profile row per candidate

Likely contents:
- candidate identity
- LinkedIn identifiers and URLs
- headline
- summary
- location
- current-role cache fields if we choose to keep them
- source metadata

### `candidate_emails_v2`
Purpose:
- One row per candidate email

Likely contents:
- candidate ID
- raw email
- normalized email
- source
- verification metadata
- primary flag

### `companies_v2`
Purpose:
- Canonical company directory

Likely contents:
- company identity keys
- company name
- LinkedIn identifiers and URLs
- enrichment fields
- source quality markers

### `candidate_experiences_v2`
Purpose:
- One row per candidate experience item

Likely contents:
- candidate ID
- company ID
- raw company name
- title
- description
- location
- start and end dates
- is current
- source payload

### `candidate_search_documents`
Purpose:
- Search-oriented denormalized candidate document

Likely contents:
- candidate ID
- flattened search text
- structured filter fields
- embedding column
- embedding metadata

## Open Questions For Task 1
- Which candidate fields remain in profile vs become derived fields?
- Do we preserve legacy raw LinkedIn JSON on `candidate_profiles_v2`, or keep it out of the clean model entirely?
- What is the exact company dedupe precedence?
- Should `candidate_experiences_v2` store normalized dates, raw date strings, or both?
- Do we want a separate skills table in phase 1, or fold skills into search documents only?
