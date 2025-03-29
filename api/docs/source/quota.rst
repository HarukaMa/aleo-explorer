API Quota
=================

.. note::

    The quota system is now applied to IP prefixes instead of individual IP addresses to discourage the use of
    multiple IP addresses in a pool to circumvent the quota system.
    The unauthenticated API access is provided to the Aleo community to allow easy access to the data. If you try to
    circumvent the quota system by any way, you will be permanently banned from using it. Please play fair so
    everyone could still have unauthenticated access to the API.
    Remember that you can always run your own instance of the explorer to have direct access to the database.

The explorer API uses a custom quota system to limit the total running time of requests to the API per IP prefix
(/24 for IPv4, /48 for IPv6). This is to prevent abuse of the API and to ensure that the API is available to all users.

The running time is counted by actual *wall time* used by requests, and doesn't include network latencies. Using
wall time means the used time will depend on the server load.

Currently, each IP address has a maximum quota of 5 seconds running time. The quota recovers at a rate of 0.1 seconds
per second, and will cap at the maximum value.

Every API response will include the following HTTP headers:

* ``quota-max``: The maximum quota, currently 5 seconds.
* ``quota-recover-rate``: The quota recover rate in seconds per second, currently 0.1.
* ``quota-remaining``: The remaining quota in seconds after current request.
* ``quota-used``: The used quota in seconds for current request.

If a request takes longer than time allowed, the request will be interrupted and a ``HTTP 429 Too Many Requests``
response will be returned. The response will have additional headers:

* ``retry-after``: A recommended wait time, currently calculated with ``1 / quota-recover-rate`` which equals 10 seconds.

To prevent abuse, the actual available quota for each request will decrease by 0.5 second for each additional *concurrent*
request. This means a request could time out while the returned headers indicate that there is still quota left.

The quota system is implemented in the ``APIQuotaMiddleware`` middleware class.

For feedback or questions, please use `the feedback form <https://aleoscan.io/feedback>`_.
