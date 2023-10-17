import synapseclient
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

# , ConsoleSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

trace.set_tracer_provider(
    TracerProvider(
        resource=Resource(attributes={SERVICE_NAME: "my_own_code_above_synapse_client"})
    )
)
# How to use OTLPSpanExporter:
trace.get_tracer_provider().add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
# How to use ConsoleSpanExporter - Having this enabled clutters the logs, it's not reccomended to use this:
# trace.get_tracer_provider().add_span_processor(
#     BatchSpanProcessor(ConsoleSpanExporter())
# )
tracer = trace.get_tracer("whatever_i_want_to_name_this")


@tracer.start_as_current_span("driveTesting::main")
def main():
    syn = synapseclient.Synapse(debug=True)

    # Log-in with ~.synapseConfig `authToken`
    syn.login()
    permissions_checking(syn)
    provenance_checking(syn)


@tracer.start_as_current_span("driveTesting::permissions_checking")
def permissions_checking(syn: synapseclient.Synapse):
    # bfauble userid: 3481671
    permissionForUser = syn.getPermissions("syn21683345", 3481671)
    print(permissionForUser)


@tracer.start_as_current_span("driveTesting::provenance_checking")
def provenance_checking(syn: synapseclient.Synapse):
    prov = syn.getProvenance("syn52570249")
    print(prov)


main()
