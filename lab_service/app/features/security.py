"""Login with SASL, permissions with ACLs, and encryption with TLS."""

import asyncio
import ssl
from aiokafka.errors import KafkaError
from app import config, kit
from app.kit import Report, consumer, producer, read
from app.registry import feature

GROUP = "Security"


def login(user: str, password: str) -> dict:
    return dict(
        bootstrap_servers=config.SASL_BOOTSTRAP,
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_plain_username=user,
        sasl_plain_password=password,
        request_timeout_ms=4000,
    )


async def try_write(user: str, password: str, name: str, value: bytes) -> str:
    """Returns "written", or the name of the error Kafka gave."""
    try:
        async with producer(**login(user, password)) as p:
            await p.send_and_wait(name, value=value)
        return "written"
    except KafkaError as exc:
        return type(exc).__name__


@feature(
    "sasl", GROUP, "SASL login",
    "The SECURE listener only accepts users with the right password. Users are stored by the cluster.",
    "Anyone who can reach a broker with no login can read everything. This closes that door.",
    'AIOKafkaProducer(security_protocol="SASL_PLAINTEXT",\n'
    '    sasl_mechanism="SCRAM-SHA-256",\n'
    '    sasl_plain_username="alice", sasl_plain_password="...")',
)
async def sasl(r: Report) -> None:
    right = await try_write("alice", "alice-secret", "lab.secure", b"hello from alice")
    wrong = await try_write("alice", "wrong-password", "lab.secure", b"never stored")
    nobody = await try_write("mallory", "guess", "lab.secure", b"never stored")
    r.table(
        ["User", "Password", "Result"],
        [["alice", "correct", right], ["alice", "wrong", wrong], ["mallory", "no such user", nobody]],
    )
    r.check("The correct password worked", right == "written", right)
    r.check("A wrong password was refused", wrong != "written", wrong)
    r.check("An unknown user was refused", nobody != "written", nobody)


@feature(
    "acl", GROUP, "ACL permissions",
    "Being logged in is not enough. ACLs list what each user may do, and everything else is refused.",
    "alice may use lab.secure only. bob has a valid login but no permissions at all.",
    "kafka-acls.sh --add --allow-principal User:alice \\\n"
    "  --operation Read --operation Write --topic lab.secure",
)
async def acl(r: Report) -> None:
    allowed = await try_write("alice", "alice-secret", "lab.secure", b"allowed")
    denied = await try_write("alice", "alice-secret", "lab.secret", b"denied")
    stranger = await try_write("bob", "bob-secret", "lab.secure", b"denied")
    r.table(
        ["User", "Topic", "Result"],
        [["alice", "lab.secure", allowed], ["alice", "lab.secret", denied], ["bob", "lab.secure", stranger]],
    )
    r.check("alice may write to her topic", allowed == "written", allowed)
    r.check("alice may not write to another topic", denied != "written", denied)
    r.check("bob logged in, but has no permission", stranger != "written", stranger)

    options = login("alice", "alice-secret")
    async with consumer("lab.secure", group_id="lab-secure-group", **options) as c:
        messages = await read(c, want=1, limit=10)
    r.check("alice may read her topic with her group", len(messages) >= 1, len(messages))


@feature(
    "tls", GROUP, "TLS encryption",
    "The TLS listener encrypts all traffic. Clients check the broker's certificate before they trust it.",
    "Without TLS, anyone on the network can read messages and passwords as they pass.",
    'context = ssl.create_default_context(cafile="ca.pem")\n'
    'AIOKafkaProducer(security_protocol="SSL", ssl_context=context)',
)
async def tls(r: Report) -> None:
    trusted = ssl.create_default_context(cafile=config.CA_FILE)
    reader, writer = await asyncio.open_connection("kafka-1", 29094, ssl=trusted, server_hostname="kafka-1")
    secure = writer.get_extra_info("ssl_object")
    r.table(
        ["Measure", "Value"],
        [["Protocol", secure.version()], ["Cipher", secure.cipher()[0]],
         ["Certificate for", secure.getpeercert()["subject"][0][0][1]]],
        "The real handshake with broker 1",
    )
    writer.close()
    await writer.wait_closed()
    r.check("The connection is encrypted", secure.version() in ("TLSv1.2", "TLSv1.3"), secure.version())

    refused = None
    try:
        await asyncio.open_connection("kafka-1", 29094, ssl=ssl.create_default_context(), server_hostname="kafka-1")
    except ssl.SSLError as exc:
        refused = type(exc).__name__
    r.check("A client that does not trust our certificate refused to connect", refused is not None, refused)

    options = dict(bootstrap_servers=config.TLS_BOOTSTRAP, security_protocol="SSL", ssl_context=trusted)
    marker = kit.unique("tls").encode()
    async with producer(**options) as p:
        await p.send_and_wait("lab.secure", value=marker)
    async with consumer(**options) as c:
        from aiokafka.structs import TopicPartition
        tp = TopicPartition("lab.secure", 0)
        c.assign([tp])
        end = (await c.end_offsets([tp]))[tp]
        c.seek(tp, end - 1)
        got = (await read(c, want=1))[0]
    r.check("A real message travelled through TLS and back", got.value == marker)
