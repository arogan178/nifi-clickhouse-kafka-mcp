from datetime import UTC, datetime
from typing import Literal

from ..config import NiFiConfig
from ..masking import mask_text
from ..nifi import (
    MAX_CONTENT_BYTES,
    NiFi,
    component_state,
    content_slice,
    event_details,
    flatten_parameters,
    flatten_search_results,
    flow_tree,
    health_report,
    looks_like_json,
    nifi_date,
    parameter_slice,
    strip_descriptors,
    summarize_event,
    summarize_flowfile,
    summarize_listing,
    summarize_process_group,
)
from ..paging import page, to_millis
from ..server import Toolbox, env_type

COMPONENT_PATHS = {
    "processor": "/processors/{id}",
    "connection": "/connections/{id}",
    "process-group": "/process-groups/{id}",
    "controller-service": "/controller-services/{id}",
    "input-port": "/input-ports/{id}",
    "output-port": "/output-ports/{id}",
}


def register(box: Toolbox, configs: dict[str, NiFiConfig]) -> None:
    nifi = {name: NiFi(cfg) for name, cfg in configs.items()}
    masked = box.cfg.masked_fields
    Env = env_type(list(configs))

    @box.tool
    def nifi_status(user: str, env: Env) -> dict:
        """NiFi version, controller status (threads, queued data, component counts) and cluster node summary."""
        about = nifi[env].get("/flow/about")["about"]
        return {
            "version": about.get("version"),
            "controller": nifi[env].get("/flow/status")["controllerStatus"],
            "cluster": nifi[env].get("/flow/cluster/summary")["clusterSummary"],
        }

    @box.tool
    def nifi_health(
        user: str, env: Env, process_group_id: str = "root", back_pressure_percent: int = 80, limit: int = 20
    ) -> dict:
        """Problems in a flow in one call, worst first.

        Connections at or over back_pressure_percent of their object or size threshold, invalid processors,
        controller services that are not enabled or are invalid, error bulletins, stopped processors.
        Each section has the total and at most limit items. Flow status is cached for 60 seconds.
        """
        client = nifi[env]
        services = client.get(
            f"/flow/process-groups/{process_group_id}/controller-services",
            {
                "uiOnly": "true",
                "includeAncestorGroups": "false",
                "includeDescendantGroups": "true",
                "includeReferencingComponents": "false",
            },
            timeout=120,
        )["controllerServices"]
        board = client.get("/flow/bulletin-board", {"limit": 200})["bulletinBoard"]["bulletins"]
        bulletins = [_bulletin(b) for b in board if "bulletin" in b]
        report = health_report(
            client.status(process_group_id), bulletins, services, back_pressure_percent, max(1, limit)
        )
        cluster = client.get("/flow/cluster/summary")["clusterSummary"]
        if cluster.get("clustered"):
            report["cluster"] = {
                "connected_nodes": cluster.get("connectedNodes"),
                "connected": cluster.get("connectedNodeCount"),
                "total": cluster.get("totalNodeCount"),
            }
        return report

    @box.tool
    def nifi_flow_tree(user: str, env: Env, process_group_id: str = "root", depth: int = 2) -> dict:
        """Process group hierarchy with ids, queued data and processor counts by state, down to depth levels.

        Groups below depth show only child_groups. Flow status is cached for 60 seconds.
        """
        snapshot = nifi[env].status(process_group_id)["processGroupStatus"]["aggregateSnapshot"]
        return flow_tree(snapshot, max(0, min(depth, 10)))

    @box.tool
    def nifi_search(user: str, env: Env, query: str, offset: int = 0, limit: int = 50) -> dict:
        """Search the NiFi flow for components by name, id, property or type.

        Returns one paged list; each item has a kind (processor, connection, parameter, ...) and up to 5 matches.
        """
        results = nifi[env].get("/flow/search-results", {"q": query})["searchResultsDTO"]
        return page(flatten_search_results(results, query, masked), offset, limit)

    @box.tool
    def nifi_process_group(user: str, env: Env, process_group_id: str = "root") -> dict:
        """Contents of a process group: child groups, processors, connections with queue sizes, ports."""
        return summarize_process_group(nifi[env].get(f"/flow/process-groups/{process_group_id}"))

    @box.tool
    def nifi_component(
        user: str,
        env: Env,
        kind: Literal["processor", "connection", "process-group", "controller-service", "input-port", "output-port"],
        component_id: str,
    ) -> dict:
        """Full configuration of one NiFi component, without property descriptors.

        Sensitive properties come back masked. For parameters use nifi_parameters.
        """
        return strip_descriptors(nifi[env].get(COMPONENT_PATHS[kind].format(id=component_id))["component"])

    @box.tool
    def nifi_processor_state(user: str, env: Env, processor_id: str) -> dict:
        """Stored state of a processor, e.g. the last listed timestamp or maximum column values.

        Local state is per node on a cluster. Values are masked like everything else.
        """
        state = nifi[env].get(f"/processors/{processor_id}/state")["componentState"]
        return {"processor_id": processor_id, **component_state(state, masked)}

    @box.tool
    def nifi_controller_services(
        user: str, env: Env, process_group_id: str = "root", include_descendants: bool = False
    ) -> dict:
        """Controller services of a process group and its ancestors, with their state.

        include_descendants also lists services of all child groups.
        """
        services = nifi[env].get(
            f"/flow/process-groups/{process_group_id}/controller-services",
            {
                "uiOnly": "true",
                "includeDescendantGroups": str(include_descendants).lower(),
                # Referencing components are most of the payload: 7.4 MB vs 1.3 MB on one real flow.
                "includeReferencingComponents": "false",
            },
            timeout=120,
        )
        return {
            "services": [
                {
                    "id": s["id"],
                    "name": s["component"]["name"],
                    "type": s["component"]["type"].rsplit(".", 1)[-1],
                    "state": s["component"]["state"],
                    "validation": s["component"].get("validationStatus"),
                    "process_group_id": s["component"].get("parentGroupId"),
                }
                for s in services["controllerServices"]
            ]
        }

    @box.tool
    def nifi_bulletins(user: str, env: Env, source_id: str | None = None, limit: int = 50) -> dict:
        """Recent NiFi bulletins (warnings and errors), optionally for one component."""
        params = {"limit": min(max(1, limit), 200)}
        if source_id:
            params["sourceId"] = source_id
        board = nifi[env].get("/flow/bulletin-board", params)["bulletinBoard"]
        return {"bulletins": [_bulletin(b) for b in board["bulletins"][: params["limit"]] if "bulletin" in b]}

    @box.tool
    def nifi_parameters(
        user: str,
        env: Env,
        context: str | None = None,
        name_contains: str | None = None,
        value_contains: str | None = None,
        offset: int = 0,
        limit: int = 50,
        max_value_chars: int = 2000,
    ) -> dict:
        """Parameters of all readable NiFi parameter contexts, sorted by context and name.

        context matches a context name or id exactly; name_contains and value_contains are substrings.
        Values are cut to max_value_chars (value_length is the full length); use nifi_parameter for the rest.
        Sensitive values come back masked. Data is cached for 60 seconds.
        """
        items, unreadable = flatten_parameters(
            nifi[env].parameter_contexts(), context, name_contains, value_contains, max(0, max_value_chars)
        )
        return {**page(items, offset, limit), "unreadable_contexts": unreadable}

    @box.tool
    def nifi_parameter(user: str, env: Env, context: str, name: str, offset: int = 0, length: int = 50000) -> dict:
        """One NiFi parameter's value, value[offset:offset+length]; next_offset is None at the end.

        context is a context name or id. Data is cached for 60 seconds.
        """
        return parameter_slice(nifi[env].parameter_contexts(), context, name, offset, length)

    def content(env: str, path: str, params: dict | None, offset: int, length: int) -> dict:
        offset, length = max(0, offset), max(1, length)
        limit = min(offset + length + 1, MAX_CONTENT_BYTES)
        raw, complete = nifi[env].get_bytes(path, params, limit)
        if not complete and looks_like_json(raw) and limit < MAX_CONTENT_BYTES:
            # JSON can only be masked whole.
            raw, complete = nifi[env].get_bytes(path, params, MAX_CONTENT_BYTES)
        return content_slice(raw, complete, offset, length, masked, box.cfg.show_binary)

    @box.tool
    def nifi_queue(user: str, env: Env, connection_id: str, limit: int = 100) -> dict:
        """FlowFiles at the head of a connection's queue (NiFi lists at most 100), with total queue size.

        Use an item's uuid and cluster_node_id with nifi_flowfile.
        """
        listing = nifi[env].temporary_request(f"/flowfile-queues/{connection_id}/listing-requests", "listingRequest")
        return summarize_listing(listing, max(0, limit))

    @box.tool
    def nifi_flowfile(
        user: str,
        env: Env,
        connection_id: str,
        flowfile_uuid: str,
        cluster_node_id: str | None = None,
        include_content: bool = False,
        content_offset: int = 0,
        content_length: int = 20000,
    ) -> dict:
        """Attributes of one queued FlowFile (see nifi_queue), optionally with content[offset:offset+length].

        cluster_node_id comes from nifi_queue. Content is text (JSON re-serialized with masked fields masked)
        or {"base64", "bytes"} for binary; offsets count characters of text or bytes of binary, and
        next_offset is None at the end; content_size is in bytes. Content is read up to 5 MB.
        """
        path = f"/flowfile-queues/{connection_id}/flowfiles/{flowfile_uuid}"
        params = {"clusterNodeId": cluster_node_id} if cluster_node_id else None
        result = summarize_flowfile(nifi[env].get(path, params)["flowFile"], masked)
        if include_content:
            result["content_size"] = result["size"]
            # The content repository can lose a claim that the queue still lists; keep the attributes.
            try:
                result.update(content(env, f"{path}/content", params, content_offset, content_length))
            except RuntimeError as e:
                result["content_error"] = str(e)
        return result

    @box.tool
    def nifi_provenance(
        user: str,
        env: Env,
        component_id: str | None = None,
        flowfile_uuid: str | None = None,
        start: str | None = None,
        end: str | None = None,
        limit: int = 50,
    ) -> dict:
        """Search NiFi provenance events of a component (processor, port, ...) and/or a FlowFile.

        At least one of component_id or flowfile_uuid is required. start/end: ISO timestamps or relative
        "-30m"/"-1h"/"-2d". limit is at most 1000. Use nifi_provenance_event for attributes and content.
        """
        if not component_id and not flowfile_uuid:
            raise ValueError("give component_id, flowfile_uuid or both")
        terms = {}
        if component_id:
            terms["ProcessorID"] = {"value": component_id}
        if flowfile_uuid:
            terms["FlowFileUUID"] = {"value": flowfile_uuid}
        request = {"maxResults": min(max(1, limit), 1000), "searchTerms": terms}
        now = datetime.now(UTC)
        if start:
            request["startDate"] = nifi_date(to_millis(start, now))
        if end:
            request["endDate"] = nifi_date(to_millis(end, now))
        query = nifi[env].temporary_request("/provenance", "provenance", {"provenance": {"request": request}})
        results = query.get("results") or {}
        if results.get("errors"):
            raise RuntimeError(f"NiFi provenance query failed: {'; '.join(results['errors'])}")
        return {
            "events": [summarize_event(e) for e in results.get("provenanceEvents", [])],
            "total": results.get("totalCount"),
        }

    @box.tool
    def nifi_provenance_event(
        user: str,
        env: Env,
        event_id: int,
        cluster_node_id: str | None = None,
        content_side: Literal["none", "input", "output"] = "none",
        content_offset: int = 0,
        content_length: int = 20000,
    ) -> dict:
        """One provenance event (see nifi_provenance) with attributes, previous values that changed, and content.

        cluster_node_id comes from nifi_provenance. content_side "input" or "output" adds that side's
        content[offset:offset+length], decoded as in nifi_flowfile; content_size is in bytes.
        """
        path = f"/provenance-events/{event_id}"
        params = {"clusterNodeId": cluster_node_id} if cluster_node_id else None
        event = nifi[env].get(path, params)["provenanceEvent"]
        result = event_details(event, masked)
        if content_side != "none":
            result["content_size"] = event.get(f"{content_side}ContentClaimFileSizeBytes")
            try:
                result.update(content(env, f"{path}/content/{content_side}", params, content_offset, content_length))
            except RuntimeError as e:
                result["content_error"] = str(e)
        return result


def _bulletin(b: dict) -> dict:
    return {
        "time": b["bulletin"].get("timestamp"),
        "level": b["bulletin"].get("level"),
        "node": b["bulletin"].get("nodeAddress"),
        "source": b["bulletin"].get("sourceName"),
        "source_id": b.get("sourceId"),
        "process_group_id": b.get("groupId"),
        "message": mask_text(b["bulletin"].get("message", ""))[:1000],
    }
