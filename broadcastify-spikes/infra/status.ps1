docker compose ps
docker exec -it broadcastify-postgres psql -U broadcastify -d broadcastify -c "select count(*) as samples from samples;"
docker exec -it broadcastify-postgres psql -U broadcastify -d broadcastify -c "select count(*) as feeds from feeds;"
docker exec -it broadcastify-redis redis-cli XLEN spike-events
