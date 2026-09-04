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

use crate::host::wit::bindings::local::kube::api::{
    ApiResource, CreateParams, DeleteParams, Error, EvictParams, JsonValue, ListParams, LogParams,
    PatchParams, PatchType, Preconditions, PropagationPolicy, ValidationDirective, VersionMatch,
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

impl From<&ApiResource> for KubeApiResource {
    fn from(api: &ApiResource) -> Self {
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
}

impl From<ApiResource> for KubeApiResource {
    fn from(api: ApiResource) -> Self {
        KubeApiResource::from(&api)
    }
}

impl From<PropagationPolicy> for KubePropagationPolicy {
    fn from(policy: PropagationPolicy) -> Self {
        match policy {
            PropagationPolicy::Orphan => KubePropagationPolicy::Orphan,
            PropagationPolicy::Background => KubePropagationPolicy::Background,
            PropagationPolicy::Foreground => KubePropagationPolicy::Foreground,
        }
    }
}

impl From<Preconditions> for KubePreconditions {
    fn from(p: Preconditions) -> Self {
        KubePreconditions {
            uid: p.uid,
            resource_version: p.resource_version,
        }
    }
}

impl From<DeleteParams> for KubeDeleteParams {
    fn from(dp: DeleteParams) -> Self {
        KubeDeleteParams {
            dry_run: dp.dry_run,
            grace_period_seconds: dp.grace_period_seconds,
            propagation_policy: dp.propagation_policy.map(Into::into),
            preconditions: dp.preconditions.map(Into::into),
        }
    }
}

impl From<VersionMatch> for KubeVersionMatch {
    fn from(vm: VersionMatch) -> Self {
        match vm {
            VersionMatch::Exact => KubeVersionMatch::Exact,
            VersionMatch::NotLater => KubeVersionMatch::NotOlderThan,
        }
    }
}

impl From<ListParams> for KubeListParams {
    fn from(lp: ListParams) -> Self {
        KubeListParams {
            label_selector: lp.label_selector,
            field_selector: lp.field_selector,
            timeout: lp.timeout,
            limit: lp.limit,
            continue_token: lp.continue_token,
            version_match: lp.version_match.map(Into::into),
            resource_version: lp.resource_version,
        }
    }
}

impl From<CreateParams> for KubePostParams {
    fn from(pp: CreateParams) -> Self {
        KubePostParams {
            dry_run: pp.dry_run,
            field_manager: pp.field_manager,
        }
    }
}

impl From<ValidationDirective> for KubeValidationDirective {
    fn from(vd: ValidationDirective) -> Self {
        match vd {
            ValidationDirective::Strict => KubeValidationDirective::Strict,
            ValidationDirective::Warn => KubeValidationDirective::Warn,
            ValidationDirective::Ignore => KubeValidationDirective::Ignore,
        }
    }
}

impl From<PatchParams> for KubePatchParams {
    fn from(pp: PatchParams) -> Self {
        KubePatchParams {
            dry_run: pp.dry_run,
            force: pp.force,
            field_manager: pp.field_manager,
            field_validation: pp.field_validation.map(Into::into),
        }
    }
}

pub fn to_kube_patch(
    patch_type: PatchType,
    body: JsonValue,
) -> Result<KubePatch<serde_json::Value>, Error> {
    let parsed_value: serde_json::Value = serde_json::from_str(&body)?;

    let patch = match patch_type {
        PatchType::Apply => KubePatch::Apply(parsed_value),
        PatchType::Merge => KubePatch::Merge(parsed_value),
        PatchType::Strategic => KubePatch::Strategic(parsed_value),
        PatchType::Json => {
            let patch_vec = serde_json::from_value(parsed_value)?;
            KubePatch::Json(patch_vec)
        }
    };

    Ok(patch)
}

impl From<LogParams> for KubeLogParams {
    fn from(lp: LogParams) -> Self {
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
}

impl From<EvictParams> for KubeEvictParams {
    fn from(ep: EvictParams) -> Self {
        KubeEvictParams {
            delete_options: ep.delete_options.map(Into::into),
            post_options: ep.post_options.into(),
        }
    }
}
