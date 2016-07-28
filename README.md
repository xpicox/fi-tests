# fi-tests
Transport layer with libfabric

# How to initialize libfabric:

Steps to follow:

- Query the host with: `fi_getinfo`<sup>[1]</sup>
- Open a fabric domain<sup>[2]</sup> `fid_fabric` with the function `fi_fabric`
<sup>[3]</sup>
  ```c
  int fi_fabric(struct fi_fabric_attr *attr,
      struct fid_fabric **fabric, void *context);
  ```
  A fabric domain is used by:
  
  - Access Domains<sup>[2]</sup> `fi_domain`<sup>[4]</sup>
  - Event Queues `fi_eq_open`<sup>[5]</sup>
  - Passive Endpoints `fi_passive_ep`<sup>[6]</sup>

### Server setup

The server must listen for incoming connections by
- Opening a **_passive endpoint_** `fi_passive_ep`
- Create an **_event queue_**:
  - Zero-initialize a `struct fi_eq_attr`
  - Set its **wait object**<sup>[7]</sup>
  - Call `fi_eq_open`
- *Associate* the passive endpoint with the event queue `fi_pep_bind`
- Call `fi_listen`<sup>[8]</sup> to allow the endpoint to accept incoming
  connection requests

Citing the documentation<sup>[8]</sup>:

> To accept a connection, the listening application first waits for a connection
  request event (`FI_CONNREQ`). After receiving such an event, the application
  allocates a new endpoint to accept the connection.
  This endpoint must be allocated using an fi_info structure referencing the 
  handle from this `FI_CONNREQ` event. fi_accept is then invoked with the newly 
  allocated endpoint.
  
> A successfully accepted connection request will result in the active 
  (connecting) endpoint seeing an `FI_CONNECTED` event on its associated event 
  queue.
  
> An `FI_CONNECTED` event will also be generated on the passive side for the 
  accepting endpoint once the connection has been properly established.


Down in the detail, a blocking read (`fi_eq_sread`) is made on the event 
queue waiting for a `FI_CONNREQ` event.
When the function returns:

- An **access domain** is opened with the information (`struct fi_info`) 
  contained in the `struct fi_eq_cm_entry` associated with the connection 
  request
- A **memory region** is registered `fi_mr_reg`
- An **endpoint** is created with the same `struct fi_info` of the associated
  **access domain** (At this point the application should free the fi_info)
- A **completion queue**<sup>[9]</sup> is initialized as follow:
  - Zero initialize a `struct fi_cq_attr`
  - Set the completion report format
  - Set the **wait object**<sup>[7]</sup> (`FI_WAIT_FD` for events,
    `FI_WAIT_UNSPEC` otherwise)
  - Set the minimum size of a completion queue (number of entries). 
    A value of 0 indicates that the provider may choose a default value.
  - Call `fi_cq_open`
- The **endpoint** is first bound to the **completion** and **event queues**
  (`fi_ep_bind`) and then *activated* with `fi_enable`
- `fi_accept` is called
- A synchronous read (`fi_eq_sread`) is made waiting for the `FI_CONNECTED` 
  event

### Client setup
  
The client must connect to the server with the following steps:
  
- Open an **access domain** `fi_domain`
- Create an **event queue** as described in the server setup
- Register a **memory region** `fi_mr_reg`
- Create an **endpoint** with the information gathered by `fi_get_info` at the
  start of the application
- Initialize a **completion queue** as described in the server setup
- Bind the **endpoint** to the **completion** and **event queues**
- Enable the **endpoint** with `fi_enable`
- Call `fi_conncet`
- Make a synchronous read (`fi_eq_sread`) waiting a `FI_CONNECTED` event

---

In the [pingpong] example, **receive buffers** are associated with the endpoint before
accepting/connecting (i.e. before sending the `FI_CONNECTED` event), however
> receive buffers may be associated with an endpoint anytime.

---

### Message passing





[1]:https://github.com/ofiwg/libfabric/blob/master/man/fi_getinfo.3.md#name
[2]:https://github.com/ofiwg/libfabric/blob/master/man/fabric.7.md#control-interfaces
[3]:https://github.com/ofiwg/libfabric/blob/master/man/fi_fabric.3.md#name
[4]:https://github.com/ofiwg/libfabric/blob/master/man/fi_domain.3.md#name
[5]:https://github.com/ofiwg/libfabric/blob/master/man/fi_eq.3.md#name
[6]:https://github.com/ofiwg/libfabric/blob/master/man/fi_endpoint.3.md#name
[7]:https://github.com/ofiwg/libfabric/blob/master/man/fi_eq.3.md#description
[8]:https://github.com/ofiwg/libfabric/blob/master/man/fi_cm.3.md#name
[9]:https://github.com/ofiwg/libfabric/blob/master/man/fi_cq.3.md#name
[pingpong]:rc_pingpong.c