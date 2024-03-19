![Test Results](https://img.shields.io/endpoint?url=https%3A%2F%2Fgist.githubusercontent.com%2Fjulienvanbeveren%2F070e58d1fbee37dab44796ed36c141bc%2Fraw%2Freport.json)
# pg-json-schema
With pg-json-schema I aim to create a complete json-schema validator in Postgres to allow for database enforced schema validation on jsonb fields.

Since the validation function is written in PL/pgSQL, you can just run this code on any db, even if you don't have direct access to the file system (for example on aws server postgres or other cloud providers)

One of the drawbacks of having this written in PL/pgSQL is the performance, which I will attempt to optimize once all tests pass.

## Contributions
All contributions are welcome! If you find edge cases that this function doesn't catch but should according to the json-schema spec, feel free to create an issue or make a PR with a test to include this case. This function only validates the 2020-12 spec.
