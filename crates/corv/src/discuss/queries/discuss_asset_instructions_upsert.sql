INSERT INTO discuss_asset_instructions VALUES (?1, ?2, ?3) ON CONFLICT(asset_path) DO UPDATE SET instructions=excluded.instructions, updated_at_ms=excluded.updated_at_ms;
