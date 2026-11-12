SELECT argMaxMerge(Contract)
FROM latest_deployments
WHERE ProjectId = {project:String}
GROUP BY ProjectId
