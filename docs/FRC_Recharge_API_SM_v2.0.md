# Prepaid FRC Recharge API

**Document Number:** SCMPREPAID0425\
**Version:** 2.0\
**Date:** 29th June 2026

## Copyright Information

©2025 Pyro Holdings Pvt. Ltd. All rights reserved. No part of this
document may be reproduced or transmitted in any form or by any means,
electronic or otherwise, including photocopying, reprinting or
recording, for any purpose, without the express written permission of
Pyro Holdings Pvt. Ltd.

------------------------------------------------------------------------

## 1. Introduction

This document gives the detailed information on the API to integrate
with Pyro's to Perform Prepaid FRC Transactions.

## 2. Application Protocol Interface

System uses the AAA (Authentication, Authorization & Accounting)
methodology for each request. Pyro Server first authenticates and
Authorizes the request from the requester, if authentication and
authorization are successful, then it will process the request further,
and send response as synchronization in the same requester URL.

The complete process will be accounted in the system for further
reference. The following section gives information about all the
services that supports request & response formats.

Minor changes can be done basing on the requirement in case there is no
major code change.

## 3. Authentication API

Below API is used to generate `sessionToken` and `accessToken` after
successful validation of `loginId` and `password`.

-   `sessionToken`: Token generated using below API will be useful in
    further APIs and is valid for 24hrs.
-   `accessToken`: Token generated using below API will be useful in
    further APIs and is valid for 15mins.

### Request Parameters

  Parameter    Description
  ------------ ------------------------------------
  `Apikey`     Unique key, will be shared by Pyro
  `loginId`    Vendor Wallet/Mobile Number
  `password`   MPIN of the Vendor Wallet

### Response Parameters

  -----------------------------------------------------------------------
  Parameter                           Description
  ----------------------------------- -----------------------------------
  `statusCode`                        Status code for Authentication

  `status`                            Status of the Authentication
                                      request

  `messageCtype`                      Message

  `sessiontoken`                      Token generated and will be active
                                      for 24 hours.

  `accesstoken`                       AccessToken generated and will be
                                      active for 15 mins.

  `username`                          Name of the Vendor
  -----------------------------------------------------------------------

### Request/Response Format

**URL:**

``` text
https://IP:PORT/auth-api/authentication
```

**Header Params:**

``` text
1. apiKey: xxxxxxxxxxx
```

**Request Body:**

``` json
{
  "loginId": "xxxxxxxxxxx",
  "password": "xxxx"
}
```

**Response Body:**

``` json
{
  "statusCode": 2000,
  "status": "SUCCESS",
  "message": "authentication",
  "data": {
    "sessionToken": "xxxxxxxxxxx",
    "accessToken": "xxxxxxxxxxx",
    "userName": "BSNL1"
  }
}
```

## 4. Refresh Access Token API

As the `accessToken` is valid for 15 mins, it can be refreshed every 15
minutes using this API.

### Request Parameters

  Parameter        Description
  ---------------- --------------------------------------------
  `Apikey`         Unique key, will be shared by Pyro
  `sessionToken`   Token generated through Authentication API
  `accessToken`    Token generated through Authentication API

### Response Parameters

  -----------------------------------------------------------------------
  Parameter                           Description
  ----------------------------------- -----------------------------------
  `statusCode`                        Status code for Authentication

  `status`                            Status of the Authentication
                                      request

  `message`                           Message

  `accesstoken`                       accessToken generated and will be
                                      active for 15 mins.
  -----------------------------------------------------------------------

### Request/Response Format

**URL:**

``` text
https://IP:PORT/auth-api/refresh-access-token
```

**Header Params:**

``` text
1. apiKey: xxxxxxxxxxx
2. sessionToken: xxxxxxxxxxx
3. accessToken: xxxxxxxxxxx
```

**Request Body:**

No Parameters.

**Response Body:**

``` json
{
  "statusCode": 2000,
  "status": "SUCCESS",
  "message": "refresh-access-token",
  "data": {
    "accessToken": "xxxxxxxxxxx"
  }
}
```

## 5. Recharge

### Request Parameters

  Request Parameter   Data Type        Size Description
  ------------------- ----------- --------- -----------------------
  `dealermsisdn`      String             10 Source/Vendor Msisdn
  `amount`            Double             10 Amount
  `destmsisdn`        String             10 Subscriber Msisdn
  `clientTxnId`       String        5 to 15 Vendor Transaction id
  `Mpin`              String              6 Mpin of Vendor Msisdn

### Response Parameters

  Parameter        Description
  ---------------- --------------------------------------
  `statusCode`     Status code for Authentication
  `status`         Status of the Authentication request
  `message`        Message
  `clientTxnId`    Vendor Transactionid
  `transacionId`   Pyro Transaction id.
  `serviceType`    Type of Transaction.
  `destMsisdn`     Subscriber msisdn
  `amount`         denomination

### Request/Response Format

**URL:**

``` text
https://IP:PORT/epin-vendor-api/recharge
```

**Header Params:**

``` text
1. apiKey: xxxxxxxxxxx
2. sessionToken: optional
3. accessToken: xxxxxxxxxxx
4. actionToken: xxxxxxxxxxx
```

**Request Body:**

``` json
{
  "dealerMsisdn": "9XXXXXXXXX",
  "destMsisdn": "9XXXXXXXXX",
  "amount": 199,
  "clientTxnId": "1234213",
  "mpin": "xxxxxx"
}
```

### Response Body

``` json
{
  "statusCode": 2002,
  "status": "In Process",
  "message": "Request is Registered",
  "data": {
    "clientTxnId": "123456",
    "transactionId": 1234,
    "serviceType": "TOPUP",
    "destMsisdn": "9440123456",
    "amount": 199
  }
}
```

## 6. Recharge Callback

### Request Parameters

  Parameter             Description
  --------------------- -----------------------------------
  `statusCode`          Status code for transaction
  `status`              Status of the transaction
  `message`             Message
  `clientTxnId`         Vendor Transactionid
  `transacionId`        Pyro Transaction id.
  `serviceType`         Type of Transaction.
  `destMsisdn`          Subscriber msisdn
  `Circle`              Circle of the subscriber
  `amount`              denomination
  `destBalanceBefore`   Dealer balance before transaction
  `destBalanceAfter`    Dealer balance after transaction

### Callback URL

Vendor need to share the callback URL to post the Recharge API.

### Callback Request Body

``` json
{
  "statusCode": 2000,
  "status": "SUCCESS",
  "message": "Recharge successful",
  "data": {
    "clientTxnId": "123456",
    "transactionId": 1234,
    "serviceType": "TOPUP",
    "destMsisdn": "9440123456",
    "circle": "Telangana",
    "amount": 199,
    "status": "SUCCESS",
    "dealerBalanceBefore": 2800.0,
    "dealerBalanceAfter": 2250.0
  }
}
```

## 7. Generate Action Token API

`actionToken`: Token generated using below API will be useful in further
APIs related recharge and topup where balance movement involved and is
valid for 1 minute and will be expired after single use.

### URL

``` text
https://IP:PORT/auth-api/generate-action-token
```

### Header Params

``` text
1. apiKey: xxxxxxxxxxx
2. sessionToken: optional
3. accessToken: xxxxxxxxxxx
```

### Request Body

No Parameters.

### Response Parameters

  -----------------------------------------------------------------------
  Parameter                           Description
  ----------------------------------- -----------------------------------
  `statusCode`                        Status code for Action Token API

  `status`                            Status of the ActionToken request

  `message`                           Message

  `accessToken`                       accessToken generated and will be
                                      active for 15 mins.

  `actionToken`                       actionToken generated and will be
                                      active for 1 min.
  -----------------------------------------------------------------------

### Response Body

``` json
{
  "statusCode": 2000,
  "status": "SUCCESS",
  "message": "generate-action-token",
  "data": {
    "accessToken": "xxxxxxxxxxx",
    "actionToken": "xxxxxxxxxxx"
  }
}
```

## 8. Transaction Status Check

This API is used to check the status of the Prepaid transaction and need
to be initiated in case of no callback response on any of the
transaction and need to be initiated only after minimum of 45 seconds
after transaction.

### Request Parameters

  Parameter          Description
  ------------------ -----------------------
  `transaction Id`   Pyro Transaction id
  `clinettxnId`      Vendor Transaction id

### Response Parameters

  Parameter               Description
  ----------------------- ------------------------------------------
  `statusCode`            Status code for Authentication
  `status`                Status of the Authentication request
  `messageCtype`          Message
  `clientTxnId`           Vendor Transactionid
  `transacionId`          Pyro Transaction id.
  `serviceType`           Type of Transaction.
  `destMsisdn`            Subscriber msisdn
  `Circle`                Circle of the subscriber
  `amount`                denomination
  `dealerBalanceBefore`   Balance before transaction of the dealer
  `dealerBalanceAfter`    Balance after transaction of the dealer.

### URL

``` text
https://IP:PORT/epin-vendor-api/transaction-status
```

### Header Params

``` text
1. apiKey: xxxxxxxxxxx
2. sessionToken: optional
3. accessToken: xxxxxxxxxxx
```

### Request Body

``` json
{
  "transactionId": "12345",
  "clientTxnId": "123456"
}
```

### Response Body

``` json
{
  "statusCode": 2000,
  "status": "SUCCESS",
  "message": " Recharge successful",
  "data": {
    "clientTxnId": "123456",
    "transactionId": "1234",
    "serviceType": "TOPUP",
    "destMsisdn": "9440123456",
    "circle": "Telangana",
    "amount": 199,
    "status": "SUCCESS",
    "dealerBalanceBefore": 2800.0,
    "dealerBalanceAfter": 2250.0
  }
}
```

## 9. Response Codes

  -------------------------------------------------------------------------------------------
                   Code Status           Message                 Scenario / Condition
  --------------------- ---------------- ----------------------- ----------------------------
                   2000 SUCCESS          authentication          Login success, JWT
                                                                 session+access tokens
                                                                 generated

                   5000 FAILED           Invalid API Key         apiKey header missing or
                                                                 doesn't match

                   5000 FAILED           Invalid credintials     Login ID/password not found

                   5000 FAILED           You are not authorized  User category check failed
                                         to use this service     

                   5000 FAILED           Invalid mpin            Invalid mpin

                   5000 FAILED           User blocked            `dealerStatus == Blocked`

                   5000 FAILED           In active user          `dealerStatus == INactive`

                   5000 FAILED           User not available      `dealerStatus == Purged`

                   2000 SUCCESS          generate-action-token   Both tokens valid, action
                                                                 token generated

                   5000 FAILED           Invalid access token    accessToken or sessionToken
                                                                 expired

                   5000 FAILED           Invalid credintials     accessToken header is null

                   2000 SUCCESS          Recharge successfull    Recharge executed
                                                                 successfully by IN system

                   2002 PENDING          In process              Recharge request registered,
                                                                 waiting IN response

                    400 FAILED           Bad Request + custom    Request parameter validation
                                         message                 failed

                    405 FAILED           Dealer balance is low   Wallet record not found for
                                                                 dealer

                    405 FAILED           You have less Stock and Wallet balance \< recharge
                                         it is not sufficient to amount
                                         do this recharge        

                    406 FAILED           You are Not Authorised  Dealer not found OR dealer
                                         to use this service     status != 1
                                         Please contact your     
                                         Franchise Manager       

                    500 FAILED           Some thing went wrong   Unexpected exception during
                                                                 recharge execution

                    505 FAILED           Sorry You have entered  Subscriber MSISDN not found
                                         invalid Mobile Number.  
                                         Please check and try    
                                         again                   

                   5005 FAILED           Recharge failed         IN (Intelligent Network)
                                                                 system returned failure

                   5007 FAILED           You are Not Authorised  Dealer not found OR wallet
                                         to use this service     balance parse error
                                         Please contact BSNL     

                   5010 FAILED           Recharge failed. Only   FRC validation failed
                                         First Recharge (FRC)    
                                         accounts or plans are   
                                         allowed.                

                   5011 FAILED           You have Entered        No RechargePlan for
                                         invalid Denomination    amount+circle or productCode

                   5012 FAILED           Sorry You have Entered  MPIN doesn't match dealer's
                                         Invalid Mpin            stored MPIN

                   5016 FAILED           Recharge request not    Same dest MSISDN + same
                                         allowed for same        amount within block time
                                         subscriber within 10    
                                         minutes                 

                   5017 FAILED           Invalid clientTxnId     clientTxnId null/empty or
                                                                 greater than 45 chars
                                                                 (non-APP channel)

                   5018 FAILED           Duplicate clientTxnId   clientTxnId already exists
                                                                 in DB for this dealer

                   5030 FAILED           Denomination is not     Service class / card-group
                                         eligible for the        validation failed
                                         Subscriber              
  -------------------------------------------------------------------------------------------

## Note

Request and Response Parameters are in **Encrypted in 3DES**. Final
Response on callback will be decrypted.
