use anyhow::{Context, Result, anyhow};
use simd_json::base::ValueAsScalar;
use simd_json::derived::TypedScalarValue;
use arrow::datatypes::i256;

pub fn get_tape_array_of_u64(
    obj: &simd_json::tape::Object<'_, '_>,
    name: &str,
) -> Result<Option<Vec<u64>>> {
    let arr = match obj.get(name) {
        None => return Ok(None),
        Some(v) if v.is_null() => return Ok(None),
        Some(v) => v,
    };
    let arr = arr
        .as_array()
        .with_context(|| format!("{} as array", name))?;

    let mut out = Vec::with_capacity(arr.len());

    for v in arr.iter() {
        let v = v
            .as_u64()
            .with_context(|| format!("element of {} as u64", name))?;
        out.push(v);
    }

    Ok(Some(out))
}

pub fn get_tape_array_of_hex(
    obj: &simd_json::tape::Object<'_, '_>,
    name: &str,
) -> Result<Option<Vec<Vec<u8>>>> {
    let arr = match obj.get(name) {
        None => return Ok(None),
        Some(v) if v.is_null() => return Ok(None),
        Some(v) => v,
    };
    let arr = arr
        .as_array()
        .with_context(|| format!("{} as array", name))?;

    let mut out = Vec::with_capacity(arr.len());

    for v in arr.iter() {
        let v = v
            .as_str()
            .with_context(|| format!("element of {} as str", name))?;
        let v =
            decode_prefixed_hex(v).with_context(|| format!("decode element of {} as hex", name))?;
        out.push(v);
    }

    Ok(Some(out))
}

pub fn get_tape_u8(obj: &simd_json::tape::Object<'_, '_>, name: &str) -> Result<Option<u8>> {
    let val = match obj.get(name) {
        None => return Ok(None),
        Some(v) if v.is_null() => return Ok(None),
        Some(v) => v,
    };
    val.as_u8()
        .with_context(|| format!("{} as u8", name))
        .map(Some)
}

pub fn get_tape_string(obj: &simd_json::tape::Object<'_, '_>, name: &str) -> Result<Option<String>> {
    let val = match obj.get(name) {
        None => return Ok(None),
        Some(v) if v.is_null() => return Ok(None),
        Some(v) => v,
    };
    val.as_str()
        .with_context(|| format!("{} as str", name))
        .map(|x| Some(x.to_owned()))
}

pub fn get_tape_i256(obj: &simd_json::tape::Object<'_, '_>, name: &str) -> Result<Option<i256>> {
    let hex = get_tape_hex(obj, name).context("get_tape_hex")?;

    hex.map(|v| i256_from_be_slice(&v).with_context(|| format!("parse i256 from {}", name)))
        .transpose()
}

pub fn i256_from_be_slice(data: &[u8]) -> Result<i256> {
    if data.len() > 32 {
        return Err(anyhow!("data is bigger than 32 bytes"));
    }

    let mut bytes = [0; 32];
    bytes[32 - data.len()..].copy_from_slice(data);

    let val = i256::from_be_bytes(bytes);

    if val.is_negative() {
        return Err(anyhow!("value was out of range"));
    }

    Ok(val)
}

pub fn get_tape_u64(obj: &simd_json::tape::Object<'_, '_>, name: &str) -> Result<Option<u64>> {
    let val = match obj.get(name) {
        None => return Ok(None),
        Some(v) if v.is_null() => return Ok(None),
        Some(v) => v,
    };
    val.as_u64()
        .with_context(|| format!("get {} as u64", name))
        .map(Some)
}

pub fn get_tape_hex(obj: &simd_json::tape::Object<'_, '_>, name: &str) -> Result<Option<Vec<u8>>> {
    let hex = match obj.get(name) {
        None => return Ok(None),
        Some(v) if v.is_null() => return Ok(None),
        Some(v) => v,
    };
    let hex = hex.as_str().with_context(|| format!("{} as str", name))?;

    decode_prefixed_hex(hex)
        .with_context(|| format!("prefix_hex_decode {}", name))
        .map(Some)
}

pub fn decode_prefixed_hex(val: &str) -> Result<Vec<u8>> {
    let val = val.strip_prefix("0x").context("invalid hex prefix")?;

    if val.len() % 2 == 0 {
        decode_hex(val)
    } else {
        let val = format!("0{val}");
        decode_hex(val.as_str())
    }
}

pub fn decode_hex(hex: &str) -> Result<Vec<u8>> {
    let len = hex.as_bytes().len();
    let mut dst = vec![0; len / 2];

    faster_hex::hex_decode(hex.as_bytes(), &mut dst)?;

    Ok(dst)
}
