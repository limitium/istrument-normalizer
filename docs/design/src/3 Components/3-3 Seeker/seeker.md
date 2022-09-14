This component does actual lookup of the instrument in external providers. Depends on the information available in the request, lookup can be confident for a particular provider or use a "guessing" approach.

## Instrument lookup 

```java
securityIdSource = guessSecurityIdSource(securityId); // simplified euristics from Instrument keeper

instrument = lookupEquity(securityId, securityIdSource);
if(instrument != null) return instrument;

instrument = lookupWarrant(securityId, securityIdSource);
if(instrument != null) return instrument;

instrument = lookupDelta1(securityId, securityIdSource);
if(instrument != null) return instrument;
```

## External providers 

All external providers are available via MDDClient library 

### Equity

```java
//Request example
MDDRequest.of(MandatoryParams.of())
    .entityKind("get_instrument")
    .param("key", id)
    .param("key_type", StringUtils.toRootLowerCase(idSource))
    .paramList("fields", EQUITY_FIELD_LIST)
    .param("result", MDDUtil.RESULT_NAME))
```

### Warrant

```java
//Request example
MDDRequest.of(MandatoryParams.of())
    .entityKind("get_warrant_reference_fields")
    .param(idSource, id)
    .param("profile", PROFILE_GBA)
    .paramList("fields", EQUITY_FIELD_LIST)
    .param("result", MDDUtil.RESULT_NAME)
```

### Delta1

Only RIC securityIdSource is supported

```java
//Request example
MDDRequest.of(MandatoryParams.of())
    .entityKind("get_index")
    .param("key", id)
    .param("key_type", "emden_ric")
    .paramList("fields", EQUITY_FIELD_LIST)
    .param("result", MDDUtil.RESULT_NAME)
```