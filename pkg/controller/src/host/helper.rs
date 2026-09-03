use anyhow::Result;
use kube::api::{
    ApiResource as KubeApiResource, DeleteParams as KubeDeleteParams,
    EvictParams as KubeEvictParams, ListParams as KubeListParams, LogParams as KubeLogParams,
    Patch as KubePatch, PatchParams as KubePatchParams, PostParams as KubePostParams,
    Preconditions as KubePreconditions, PropagationPolicy as KubePropagationPolicy,
    ValidationDirective as KubeValidationDirective, VersionMatch as KubeVersionMatch,
};
use kube::core::{dynamic::DynamicObject, metadata::PartialObjectMeta};
use kube::{Api, Client};

use crate::host::api::bindings::local::kube::api::{
    ApiResource, CreateParams, DeleteParams, Error, EvictParams, HttpError, JsonValue, ListParams,
    LogParams, PatchParams, PatchType, Preconditions, PropagationPolicy, ValidationDirective,
    VersionMatch,
};

pub fn get_dynamic_api(
    client: Client,
    namespace: Option<String>,
    api_res: &KubeApiResource,
) -> Api<DynamicObject> {
    if let Some(ns) = namespace {
        Api::namespaced_with(client.clone(), &ns, api_res)
    } else {
        Api::all_with(client.clone(), api_res)
    }
}

pub fn get_meta_api(
    client: Client,
    api: &ApiResource,
    api_res: &KubeApiResource,
) -> Api<PartialObjectMeta<DynamicObject>> {
    if let Some(ns) = &api.namespace {
        Api::namespaced_with(client.clone(), ns, api_res)
    } else {
        Api::all_with(client.clone(), api_res)
    }
}

pub fn to_kube_api_resource(api: &ApiResource) -> KubeApiResource {
    let api_version = if api.group.is_empty() {
        api.version.clone()
    } else {
        format!("{}/{}", api.group, api.version)
    };
    KubeApiResource {
        group: api.group.clone(),
        version: api.version.clone(),
        api_version,
        kind: api.kind.clone(),
        plural: api.plural.clone(),
    }
}

pub fn to_kube_propagation_policy(policy: PropagationPolicy) -> KubePropagationPolicy {
    match policy {
        PropagationPolicy::Orphan => KubePropagationPolicy::Orphan,
        PropagationPolicy::Background => KubePropagationPolicy::Background,
        PropagationPolicy::Foreground => KubePropagationPolicy::Foreground,
    }
}

pub fn to_kube_preconditions(p: Preconditions) -> KubePreconditions {
    KubePreconditions {
        uid: p.uid,
        resource_version: p.resource_version,
    }
}

pub fn to_kube_delete_params(dp: DeleteParams) -> KubeDeleteParams {
    KubeDeleteParams {
        dry_run: dp.dry_run,
        grace_period_seconds: dp.grace_period_seconds,
        propagation_policy: dp.propagation_policy.map(to_kube_propagation_policy),
        preconditions: dp.preconditions.map(to_kube_preconditions),
    }
}

pub fn to_kube_version_match(vm: VersionMatch) -> KubeVersionMatch {
    match vm {
        VersionMatch::Exact => KubeVersionMatch::Exact,
        VersionMatch::NotLater => KubeVersionMatch::NotOlderThan,
    }
}

pub fn to_kube_list_params(lp: ListParams) -> KubeListParams {
    KubeListParams {
        label_selector: lp.label_selector,
        field_selector: lp.field_selector,
        timeout: lp.timeout,
        limit: lp.limit,
        continue_token: lp.continue_token,
        version_match: lp.version_match.map(to_kube_version_match),
        resource_version: lp.resource_version,
    }
}

pub fn to_kube_post_params(pp: CreateParams) -> KubePostParams {
    KubePostParams {
        dry_run: pp.dry_run,
        field_manager: pp.field_manager,
    }
}

pub fn to_kube_validation_directive(vd: ValidationDirective) -> KubeValidationDirective {
    match vd {
        ValidationDirective::Strict => KubeValidationDirective::Strict,
        ValidationDirective::Warn => KubeValidationDirective::Warn,
        ValidationDirective::Ignore => KubeValidationDirective::Ignore,
    }
}

pub fn to_kube_patch_params(pp: PatchParams) -> KubePatchParams {
    KubePatchParams {
        dry_run: pp.dry_run,
        force: pp.force,
        field_manager: pp.field_manager,
        field_validation: pp.field_validation.map(to_kube_validation_directive),
    }
}

pub fn to_kube_patch(
    patch_type: PatchType,
    body: JsonValue,
) -> Result<KubePatch<serde_json::Value>, Error> {
    let parsed_value: serde_json::Value = serde_json::from_str(&body).map_err(to_serde_error)?;

    let patch = match patch_type {
        PatchType::Apply => KubePatch::Apply(parsed_value),
        PatchType::Merge => KubePatch::Merge(parsed_value),
        PatchType::Strategic => KubePatch::Strategic(parsed_value),
        PatchType::Json => {
            let patch_vec = serde_json::from_value(parsed_value).map_err(to_serde_error)?;
            KubePatch::Json(patch_vec)
        }
    };

    Ok(patch)
}

pub fn to_kube_log_params(lp: LogParams) -> KubeLogParams {
    KubeLogParams {
        container: lp.container,
        follow: lp.follow,
        limit_bytes: lp.limit_bytes.map(|x| x as i64),
        pretty: lp.pretty,
        previous: lp.previous,
        since_seconds: lp.since_seconds.map(|x| x as i64),
        since_time: lp.since_time.and_then(|t| t.parse().ok()),
        tail_lines: lp.tail_lines.map(|x| x as i64),
        timestamps: lp.timestamps,
    }
}

pub fn to_kube_evict_params(ep: EvictParams) -> KubeEvictParams {
    KubeEvictParams {
        delete_options: ep.delete_options.map(to_kube_delete_params),
        post_options: to_kube_post_params(ep.post_options),
    }
}

pub fn to_wit_error<E>(err: E) -> Error
where
    E: Into<anyhow::Error>,
{
    let anyhow_err: anyhow::Error = err.into();

    if let Some(kube_err) = anyhow_err.downcast_ref::<kube::Error>() {
        match kube_err {
            kube::Error::Api(status) => {
                if status.code == 404 {
                    return Error::NotFound;
                }
                return Error::Http(HttpError {
                    code: status.code,
                    reason: status.reason.clone(),
                    message: status.message.clone(),
                });
            }
            _ => {}
        }
    }

    Error::Other(anyhow_err.to_string())
}

pub fn to_serde_error(err: serde_json::Error) -> Error {
    Error::Other(err.to_string())
}
