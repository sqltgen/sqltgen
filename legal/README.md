# sqltgen License Files

This folder contains the licensing terms for the sqltgen project.

## Files

- **LICENSE** — Apache License, Version 2.0, with two custom appendices
- **NOTICE** — Author and contributor attribution information
- **TRADEMARKS.md** — Trademark and brand usage policy

## Quick Summary

sqltgen is licensed under **Apache License 2.0** with two additional provisions:

### A. Generated Output

**Ownership:** Generated code is owned by whoever owns the input (your schemas, SQL files, configuration). You can use, modify, and distribute it under any license you choose—proprietary, commercial, closed-source, etc. No Apache license applies.

**Exception:** Code that is Apache-2.0 licensed remains so if:
- You manually copy sqltgen source code into your generated output
- You deliberately engineer your inputs to make sqltgen emit code matching sqltgen's templates or patterns

### B. Authorship Misrepresentation

You cannot falsely claim authorship of sqltgen when distributing a modified version (fork). You must identify the original authors and clearly mark your modifications.

## What This Means

| Use Case | Allowed? | Notes |
|---|---|---|
| Use sqltgen to generate code | ✅ | Generated output is owned by whoever owns the input |
| Distribute your generated code | ✅ | No Apache license applies to your generated code |
| Copy sqltgen source code into your output | ❌ | That portion remains Apache-2.0 licensed |
| Deliberately engineer inputs to emit licensed code | ❌ | Output from deliberate circumvention remains Apache-2.0 licensed |
| Fork sqltgen and modify it | ✅ | You must credit original authors and clearly mark your changes |
| Falsely claim authorship of sqltgen | ❌ | You must identify original authors when distributing a fork |

## For Contributors

By submitting a contribution to sqltgen, you agree that your contribution is licensed under the Apache License, Version 2.0.

## Questions?

Refer to the full LICENSE file for complete terms and conditions.
