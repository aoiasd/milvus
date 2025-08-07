use serde_json as json;
use std::path::PathBuf;
use crate::error::{Result, TantivyBindingError};

// pub fn build_resource_path(path: &str, system_params: &json::Map<String, json::Value>) -> Result<PathBuf> {
//     let path_obj = PathBuf::new(path);
    
//     // Get base directory from system_params["dict_build_dir"]
//     let base_dir = system_params.get("dict_build_dir")
//         .and_then(|v| v.as_str())
//         .ok_or(TantivyBindingError::InvalidArgument("Missing dict_build_dir in system_params".to_string()))?;
    
//     // Check if the path is absolute
//     if path_obj.is_absolute() {
//         // For absolute path, return {base_dir}/file/{path}
//         let new_path = PathBuf::new(base_dir).join("file").join(path.trim_start_matches('/'));
//         return Ok(new_path);
//     }
    
//     // Handle relative path
//     // Extract the first component as resource name
//     let mut components = path_obj.components();
//     let first_component = match components.next() {
//         Some(std::path::Component::Normal(name)) => name.to_string_lossy(),
//         _ => return Err(TantivyBindingError::InvalidArgument("Invalid path format".to_string())),
//     };
    
//     // Get the corresponding resourceID from system_params["resources"]
//     let resources = system_params.get("resources")
//         .and_then(|v| v.as_object())
//         .ok_or(TantivyBindingError::InvalidArgument("Missing or invalid resources in system_params".to_string()))?;
    
//     let resource_id = resources.get(first_component.as_ref())
//         .and_then(|v| v.as_str())
//         .ok_or(TantivyBindingError::InvalidArgument(format!("Resource '{}' not found in system_params", first_component)))?;
    
//     // Build the remaining path components
//     let remaining_path: Vec<_> = components.collect();
//     let mut new_path = Path::new(base_dir).join("resources").join(resource_id);
    
//     // Add the remaining path components
//     for component in remaining_path {
//         if let std::path::Component::Normal(name) = component {
//             new_path = new_path.join(name);
//         }
//     }
    
//     Ok(new_path)
// }