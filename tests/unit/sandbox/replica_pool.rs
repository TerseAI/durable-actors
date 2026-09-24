use super::*;
use crate::postgres::testing::with_postgres;

#[tokio::test]
async fn retrying_a_replica_claim_reuses_the_reservation_and_pool_cleanup_preserves_witnesses()
-> Result<()> {
    with_postgres(async |fixture| {
        let database = PostgresDatabase::connect(&fixture.url).await?;
        let replicas = PoolStore(database.clone(), SpareKind::Replica);
        let name = super::super::tests::reserve(&replicas, "replicas", 1).await?.unwrap();
        let handle = SpareHandle { name, resource_id: "sb-replica".into(), route: "https://replica.test".into(), canonical_region: "region".into(), control_route: "https://assign.test".into(), control_token: "secret".into() };
        replicas.publish("replicas", &handle, 600).await?;
        let claims = futures_util::future::try_join_all((0..8).map(|_| replicas.reserve_replica("replicas", "replica-host"))).await?;
        for claim in claims {
            assert!(matches!(claim, Reservation::Ready(spare) if spare == handle));
        }
        database.execute("UPDATE durable_actors_spares SET status = 'active', expires_at = clock_timestamp() - interval '1 second'", &[]).await?;
        PoolStore(database.clone(), SpareKind::Actor).retire_unwanted(&[], false).await?;
        replicas.retire_unwanted(&[], false).await?;
        assert!(replicas.retiring().await?.is_empty(), "only replica lifecycle recovery may retire an assigned witness");
        let cold = replicas.reserve_replica("replicas", "another-host").await?;
        assert!(matches!(cold, Reservation::Create(_)));
        assert!(matches!(replicas.reserve_replica("replicas", "another-host").await?, Reservation::Pending));
        Ok(())
    }).await
}
