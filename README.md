# `redb` Transaction Experiment

This program attempts to answer the question: within the context of a transaction, do reads see up-to-date data?

```
pre-commit write txn sees updated data, hooray!
concurrent read txn correctly sees initial value
post-commit existing read txn INCORRECTLY sees initial value
post-commit new read txn correctly sees updated value
```

Short answer: yes, but we need to keep read transactions short/local in order to see updated data.
