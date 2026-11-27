use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

use super::{ActorInstanceOverview, ActorResidency};

#[derive(Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum InstanceSort {
    Instance,
    Sockets,
    #[default]
    LastActive,
    Disk,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InstancePageQuery {
    pub actor_name: String,
    pub actor_id: Option<String>,
    #[serde(default)]
    pub search: String,
    pub status: Option<ActorResidency>,
    #[serde(default)]
    pub sort: InstanceSort,
    #[serde(default = "descending")]
    pub descending: bool,
    #[serde(default)]
    pub offset: u32,
    #[serde(default = "page_size")]
    pub limit: u32,
}

fn descending() -> bool {
    true
}

fn page_size() -> u32 {
    25
}

#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InstancePage {
    pub live: u64,
    pub dormant: u64,
    pub unknown: u64,
    pub total: u64,
    pub instances: Vec<InstanceRow>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub methods: Option<Vec<String>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InstanceRow {
    pub actor_id: String,
    pub status: ActorResidency,
    pub last_active_at_ms: Option<u64>,
    pub database_bytes: Option<u64>,
    pub socket_count: u64,
}

impl InstanceRow {
    pub(crate) fn from_overview(instance: ActorInstanceOverview, socket_count: u64) -> Self {
        Self {
            actor_id: instance.actor_id,
            status: instance.status,
            last_active_at_ms: instance.last_active_at_ms,
            database_bytes: instance.database_bytes,
            socket_count,
        }
    }

    pub(crate) fn socket_only(actor_id: String, socket_count: u64) -> Self {
        Self {
            actor_id,
            status: ActorResidency::Dormant,
            last_active_at_ms: None,
            database_bytes: None,
            socket_count,
        }
    }
}

impl InstancePage {
    pub(crate) fn from_rows(mut rows: Vec<InstanceRow>, query: &InstancePageQuery) -> Self {
        let mut page = Self::default();
        let search = query.search.to_lowercase();
        rows.retain(|row| {
            if query
                .actor_id
                .as_ref()
                .is_some_and(|id| id != &row.actor_id)
            {
                return false;
            }
            match row.status {
                ActorResidency::Live => page.live += 1,
                ActorResidency::Dormant => page.dormant += 1,
                ActorResidency::Unknown => page.unknown += 1,
            }
            query.status.is_none_or(|status| status == row.status)
                && (search.is_empty() || row.actor_id.to_lowercase().contains(&search))
        });
        page.total = rows.len() as u64;
        query.sort_rows(&mut rows);
        page.instances = rows
            .into_iter()
            .skip(query.offset as usize)
            .take(query.limit as usize)
            .collect();
        page
    }
}

impl InstancePageQuery {
    pub(crate) fn sort_rows(&self, rows: &mut [InstanceRow]) {
        let order = |value: Ordering| {
            if self.descending {
                value.reverse()
            } else {
                value
            }
        };
        let metric = |a: Option<u64>, b: Option<u64>| match (a, b) {
            (Some(a), Some(b)) => order(a.cmp(&b)),
            _ => b.is_some().cmp(&a.is_some()),
        };
        rows.sort_by(|a, b| {
            let result = match self.sort {
                InstanceSort::Instance => order(a.actor_id.cmp(&b.actor_id)),
                InstanceSort::Sockets => order(a.socket_count.cmp(&b.socket_count)),
                InstanceSort::LastActive => metric(a.last_active_at_ms, b.last_active_at_ms),
                InstanceSort::Disk => metric(a.database_bytes, b.database_bytes),
            };
            result.then_with(|| a.actor_id.cmp(&b.actor_id))
        });
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ActorTypesQuery {
    #[serde(default)]
    pub offset: u32,
    #[serde(default = "page_size")]
    pub limit: u32,
}

#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ActorTypesPage {
    pub live: u64,
    pub dormant: u64,
    pub total: u64,
    pub largest: u64,
    pub open_sockets: u64,
    pub gateways: std::collections::BTreeMap<String, u64>,
    pub actors: Vec<ObservedActorType>,
}

#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ObservedActorType {
    pub actor_name: String,
    pub live: u64,
    pub dormant: u64,
    pub unknown: u64,
}

impl ObservedActorType {
    pub(crate) fn count(&mut self, status: ActorResidency) {
        match status {
            ActorResidency::Live => self.live += 1,
            ActorResidency::Dormant => self.dormant += 1,
            ActorResidency::Unknown => self.unknown += 1,
        }
    }

    fn total(&self) -> u64 {
        self.live + self.dormant + self.unknown
    }
}

impl ActorTypesPage {
    pub(crate) fn from_types(mut actors: Vec<ObservedActorType>, query: &ActorTypesQuery) -> Self {
        actors.sort_by(|a, b| {
            b.total()
                .cmp(&a.total())
                .then_with(|| a.actor_name.cmp(&b.actor_name))
        });
        let live = actors.iter().map(|actor| actor.live).sum();
        let dormant = actors.iter().map(|actor| actor.dormant).sum();
        let total = actors.len() as u64;
        let largest = actors.first().map_or(0, ObservedActorType::total);
        Self {
            live,
            dormant,
            total,
            largest,
            actors: actors
                .into_iter()
                .skip(query.offset as usize)
                .take(query.limit as usize)
                .collect(),
            ..Self::default()
        }
    }
}
