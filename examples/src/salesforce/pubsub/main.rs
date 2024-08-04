use flowgen_salesforce::eventbus::v1::{pub_sub_client::PubSubClient, SchemaRequest, TopicRequest};
use oauth2::basic::BasicClient;
use oauth2::reqwest::async_http_client;
use oauth2::{AuthUrl, ClientId, ClientSecret, TokenResponse, TokenUrl};
use std::env;
use tonic::{metadata::MetadataValue, transport::ClientTlsConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Setup environment variables
    let sfdc_credentials = env!("SALESFORCE_CREDENTIALS");
    let sfdc_instance_url = env!("SALESFORCE_INSTANCE_URL");
    let sfdc_tenant_id = env!("SALESFORCE_TENANT_ID");
    let sfdc_topic_name = env!("SALESFORCE_TOPIC_NAME");

    let tls_config = ClientTlsConfig::new();
    let channel_endpoint =
        format!("{0}:443", flowgen_salesforce::eventbus::GLOBAL_ENDPOINT).to_string();
    let channel = tonic::transport::Channel::from_shared(channel_endpoint)?
        .tls_config(tls_config)?
        .connect()
        .await?;

    let sfdc_client = flowgen_salesforce::auth::Client::builder()
        .with_credetentials_path(sfdc_credentials.into())
        .build();

    let auth_client = BasicClient::new(
        ClientId::new(sfdc_client.client_id),
        Some(ClientSecret::new(sfdc_client.client_secret)),
        AuthUrl::new(sfdc_client.auth_url)?,
        Some(TokenUrl::new(sfdc_client.token_url)?),
    );

    let token_result = auth_client
        .exchange_client_credentials()
        .request_async(async_http_client)
        .await?;

    let auth_header: MetadataValue<_> = token_result.access_token().secret().parse()?;

    let mut client = PubSubClient::with_interceptor(channel, move |mut req: tonic::Request<()>| {
        req.metadata_mut()
            .insert("accesstoken", auth_header.clone());
        req.metadata_mut()
            .insert("instanceurl", MetadataValue::from_static(sfdc_instance_url));
        req.metadata_mut()
            .insert("tenantid", MetadataValue::from_static(sfdc_tenant_id));
        Ok(req)
    });

    let topic_resp = client
        .get_topic(tonic::Request::new(TopicRequest {
            topic_name: String::from(sfdc_topic_name),
        }))
        .await?;

    let _schema_info = client
        .get_schema(tonic::Request::new(SchemaRequest {
            schema_id: topic_resp.into_inner().schema_id,
        }))
        .await?
        .into_inner();

    Ok(())
}
