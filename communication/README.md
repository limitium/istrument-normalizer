## gRPC and messages
Temporarily this module owns all flatbuffers schemas and is responsible for compiling and publishing artifacts

### Build
From root project dir
```shell
./gradlew :messages:build
```
### Include shared definitions
Due to the {link to GH issue} flatbuffers cannot properly resolve namespaces, so the non flatbuffers directive is used for include statements
```
include path/to/file.fbs
```
It must be used after `namespace` definition and basically replaces line with the file's content


### File name conventions
+ `CamelCase.fbs` files are entry points for generation
+ `lowercase.fbs` files are shared included files, without namespaces

### Module structure
`./flatc` - flatbuffers compiler `v2.0.3`


All schema definitions are stored in

`src/main/flatbuffers`

with the following structure

```text
.
├── grpc - gRPC endpoint definitions
├── messages
│  ├── domain - describes business events
│  │   ├── execution-reports - group of execution related events
│  │   └── ... - other business groups
│  └── internal - describes projects internal messages
│      ├── instrument-normalizer - owner on messages
│      └── ... - other projects
└── tables - for common definitions
```
