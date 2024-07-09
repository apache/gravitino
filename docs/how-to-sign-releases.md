---
title: How to sign and verify Gravitino releases
slug: /how-to-sign-releases
license: "This software is licensed under the Apache License version 2."
---

These instructions provide a guide to signing and verifying Apache Gravitino releases to enhance the security of releases. A signed release enables people to confirm the author of the release and guarantees that the code hasn't been altered.

## Prerequisites

Before signing or verifying a Gravitino release, ensure you have the following prerequisites installed:

- GPG/GnuPG
- Release artifacts

## Platform support

 These instructions are for macOS. You may need to make adjustments for other platforms.

1. **How to Install GPG or GnuPG:**

   [GnuPG](https://www.gnupg.org) is an open-source implementation of the OpenPGP standard and allows you to encrypt and sign files or emails. GnuPG, also known as GPG, is a command line tool.

   Check to see if GPG is installed by running the command:

   ```shell
   gpg -help
   ```

   If GPG/GnuPG isn't installed, run the following command to install it. You only need to do this step once.

    ```shell
    brew install gpg
    ```

## Signing a release

1. **Create a Public/Private Key Pair:**

    Check to see if you already have a public/private key pair by running the command:

    ```shell
    gpg --list-secret-keys
    ```

    If you get no output, you'll need to generate a public/private key pair.

    Use this command to generate a public/private key pair. This is a one-time process. Setting the key expiry to 5 years and omitting a comment. All other defaults are acceptable.

    ```shell
    gpg --full-generate-key
    ```

    Here is an example of generating a public/private key pair by using the previous command.

    ```shell
    gpg (GnuPG) 2.4.3; Copyright (C) 2023 g10 Code GmbH
    This is free software: you are free to change and redistribute it.
    There is NO WARRANTY, to the extent permitted by law.

    Please select what kind of key you want:
    (1) RSA and RSA
    (2) DSA and Elgamal
    (3) DSA (sign only)
    (4) RSA (sign only)
    (9) ECC (sign and encrypt) *default*
    (10) ECC (sign only)
    (14) Existing key from card
    Your selection?
    Please select which elliptic curve you want:
    (1) Curve 25519 *default*
    (4) NIST P-384
    (6) Brainpool P-256
    Your selection?
    Please specify how long the key should be valid.
            0 = key does not expire
        <n>  = key expires in n days
        <n>w = key expires in n weeks
        <n>m = key expires in n months
        <n>y = key expires in n years
    Key is valid for? (0) 5y
    Key expires at Mon 13 Nov 16:08:58 2028 AEDT
    Is this correct? (y/N) y

    GnuPG needs to construct a user ID to identify your key.

    Real name: John Smith
    Email address: john@apache.org
    Comment:
    You selected this USER-ID:
        "John Smith <john@apache.org>"

    Change (N)ame, (C)omment, (E)mail or (O)kay/(Q)uit? o
    We need to generate a lot of random bytes. It is a good idea to perform
    some other action (type on the keyboard, move the mouse, utilize the
    disks) during the prime generation; this gives the random number
    generator a better chance to gain enough entropy.
    We need to generate a lot of random bytes. It is a good idea to perform
    some other action (type on the keyboard, move the mouse, utilize the
    disks) during the prime generation; this gives the random number
    generator a better chance to gain enough entropy.
    gpg: revocation certificate stored as '/Users/justin/.gnupg/openpgp-revocs.d/CC6BD9B0A3A31A7ACFF9E1383DF672F671B7F722.rev'
    public and secret key created and signed.

    pub   ed25519 2023-11-15 [SC] [expires: 2028-11-13]
        CC6BD9B0A3A31A7ACFF9E1383DF672F671B7F722
    uid                      John Smith <john@apache.org>
    sub   cv25519 2023-11-15 [E] [expires: 2028-11-13]
    ```

:::caution important
Keep your private key secure and saved somewhere other than just on your computer. Don't forget your key password, and also securely record it somewhere. If you lose your keys or forget your password, you won't be able to sign releases.
:::

2. **Sign a release:**

    To sign a release, use the following command for each release file:

    ```shell
    gpg --detach-sign --armor <filename>.[zip|tar.gz]
    ```

    For example, to sign the Gravitino 0.2.0 release you would use this command.

    ```shell
    gpg --detach-sign --armor gravitino.0.2.0.zip
    ```

   This generates an .asc file containing a PGP signature. Anyone can use this file and your public signature to verify the release.

3. **Generate hashes for a release:**

    Use the following command to generate hashes for a release:

    ```shell
    shasum -a 256 <filename>.[zip|tar.gz] > <filename>.[zip|tar.gz].sha256
    ```

    For example, to generate a hash for the Gravitino 0.2.0 release you would use this command:

    ```shell
    shasum -a 256 gravitino.0.2.0.zip > gravitino.0.2.0.zip.sha256
    ```

4. **Copy your public key to the KEYS file:**

    The KEYS file contains public keys used to sign previous releases. You only need to do this step once. Execute the following command to copy your public key to a KEY file and then append your KEY to the KEYS file. The KEYS file contains all the public keys used to sign previous releases.

    ```shell
    gpg --output KEY --armor --export <youremail>
    cat KEY >> KEYS
    ```

5. **Publish hashes and signatures:**

    Upload the generated .asc and .sha256 files along with the release artifacts and KEYS file to the release area.

## Verifying a release

1. **Import public keys:**

    Download the KEYS file. Import the public keys used to sign all previous releases with this command. It doesn't matter if you have already imported the keys.

    ```shell
    gpg --import KEYS
    ```

2. **Verify the signature:**

    Download the .asc and release files. Use the following command to verify the signature:

    ```shell
    gpg --verify <filename>.[zip|tar.gz].asc
    ```

    The output should contain the text "Good signature from ...".

    For example to verify the Gravitino 0.2.0 zip file you would use this command:

    ```shell
     gpg --verify gravitino.0.2.0.zip.asc
    ```

3. **Verify the hashes:**

    Check if the hashes match, using the following command:

    ```shell
    diff -u <filename>.[zip|tar.gz].sha256 <(shasum -a 256 <filename>.[zip|tar.gz])
    ```

    For example to verify the Gravitino 2.0 zip file you would use this command:

    ```shell
    diff -u gravitino.0.2.0.zip.sha256 <(shasum -a 256 gravitino.0.2.0.zip)
    ```

    This command ensures that the signatures match and that there are no differences between them.
