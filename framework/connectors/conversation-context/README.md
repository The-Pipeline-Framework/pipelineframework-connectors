# Conversation context contract

This module contributes typed conversation and context-assembly values. It has no transcript store, database schema,
or hosted session. An application owns the approved turn record, storage, retention, authorization, and Query/Command
bindings. The connector neither discovers the application's persistence setup nor selects a fallback store.

An application can persist its authored turn entity with TPF's persistence aspect and query it through its read model.
The aspect is a pass-through write boundary; it does not itself define transcript pagination or erasure. Where the
application needs explicit append/delete Commands, it implements them against its chosen store with stable effect
identity and an authorized scope. TPF's `CommandEffectStore` owns the logical Command receipt. Reading a transcript
does not implicitly add it to an LLM Query: the application explicitly chooses and bounds model-visible context.
