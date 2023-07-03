API Quota
=================

The explorer API uses a custom quota system to limit the total running time of requests to the API per IP address.
This is to prevent abuse of the API and to ensure that the API is available to all users.

The running time is counted by actual *wall time* used by requests, and doesn't include network latencies. Using
wall time means the used time will depend on the server load.

Currently, each IP address has a maximum quota of 10 seconds running time. The quota recovers at a rate of 0.1 seconds
per second, and will cap at the maximum value.

Every API response will include the following HTTP headers:

* ``quota-max``: The maximum quota, currently 10 seconds.
* ``quota-recover-rate``: The quota recover rate in seconds per second, currently 0.1.
* ``quota-remaining``: The remaining quota in seconds after current request.
* ``quota-used``: The used quota in seconds for current request.

If a request takes longer than time allowed, the request will be interrupted and a ``HTTP 429 Too Many Requests``
response will be returned. The response will have additional headers:

* ``retry-after``: A recommended wait time, currently calculated with ``1 / quota-recover-rate`` which equals 10 seconds.

To prevent abuse, the actual available quota for each request will decrease by 1 second for each additional *concurrent*
request. This means a request could time out while the returned headers indicate that there is still quota left.

The quota system is implemented in the ``APIQuotaMiddleware`` middleware class.

For feedback or questions, please use `the feedback form <https://explorer.hamp.app/feedback>`_.
