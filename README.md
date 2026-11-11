# Babel Lag

**When the world learns something new, how long until it reaches each language on Wikipedia, and
which languages stay in the dark?**

Babel Lag ingests every live change on every Wikimedia wiki (EventStreams), the full MediaWiki edit
history since 2001 and per-article pageviews since 2015, links topics across language editions via
Wikidata, and measures coverage lag, update lag, demand without supply and coverage depth.

Status: milestone 1 (foundation and feasibility). See `docs/PROGRESS.md`.

Data: Wikimedia Foundation, EventStreams and the CC0 dumps at dumps.wikimedia.org. Lag is measured
on Wikipedia only and says nothing about other media in a language. Aggregates only; no editor
names or IP addresses are stored or published.

License: MIT.
