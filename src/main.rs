mod message;
mod peer;
mod vc;
mod vcreg;

use std::collections::HashMap;
use std::env;
use std::num::{NonZeroU32, NonZeroU8};

use actix_web::web::Payload;
use actix_web::{web, App, Error, HttpRequest, HttpResponse, HttpServer};
use actix_web_actors::ws;
use gql_client::Client;
use mediasoup::prelude::*;
use peer::PeerConnection;
use serde::Deserialize;
use vc::VcId;
use vcreg::VcRegistry;

#[derive(Deserialize)]
struct Data {
    me: User,
}

#[derive(Deserialize)]
struct User {
    id: String,
}

fn media_codecs() -> Vec<RtpCodecCapability> {
    vec![
        RtpCodecCapability::Audio {
            mime_type: MimeTypeAudio::Opus,
            preferred_payload_type: None,
            clock_rate: NonZeroU32::new(48000).unwrap(),
            channels: NonZeroU8::new(2).unwrap(),
            parameters: RtpCodecParametersParameters::from([("useinbandfec", 1_u32.into())]),
            rtcp_feedback: vec![RtcpFeedback::TransportCc],
        },
        RtpCodecCapability::Video {
            mime_type: MimeTypeVideo::Vp8,
            preferred_payload_type: None,
            clock_rate: NonZeroU32::new(90000).unwrap(),
            parameters: RtpCodecParametersParameters::default(),
            rtcp_feedback: vec![
                RtcpFeedback::Nack,
                RtcpFeedback::NackPli,
                RtcpFeedback::CcmFir,
                RtcpFeedback::GoogRemb,
                RtcpFeedback::TransportCc,
            ],
        },
        RtpCodecCapability::Video {
            mime_type: MimeTypeVideo::Vp9,
            preferred_payload_type: None,
            clock_rate: NonZeroU32::new(90000).unwrap(),
            parameters: RtpCodecParametersParameters::default(),
            rtcp_feedback: vec![
                RtcpFeedback::Nack,
                RtcpFeedback::NackPli,
                RtcpFeedback::CcmFir,
                RtcpFeedback::GoogRemb,
                RtcpFeedback::TransportCc,
            ],
        },
        RtpCodecCapability::Video {
            mime_type: MimeTypeVideo::H265,
            preferred_payload_type: None,
            clock_rate: NonZeroU32::new(90000).unwrap(),
            parameters: RtpCodecParametersParameters::default(),
            rtcp_feedback: vec![
                RtcpFeedback::Nack,
                RtcpFeedback::NackPli,
                RtcpFeedback::CcmFir,
                RtcpFeedback::GoogRemb,
                RtcpFeedback::TransportCc,
            ],
        },
    ]
}

async fn ws_index(
    request: HttpRequest,
    worker_manager: actix_web::web::Data<WorkerManager>,
    vc_registry: actix_web::web::Data<VcRegistry>,
    stream: Payload,
) -> Result<HttpResponse, Error> {
    if request.cookies().is_err() {
        return Ok(HttpResponse::Unauthorized().finish());
    }

    let mut session = String::from("");
    for cookie in request.cookies().unwrap().iter() {
        let (name, val) = cookie.name_value();
        if name == "session" {
            session = val.to_string()
        }
    }
    if session == "" {
        return Ok(HttpResponse::Unauthorized().finish());
    }

    let endpoint = env::var("RTWALK_API").expect("RTWALK_API must be set");
    let query = r#"
       query {
           me {
               id
           }
       }
   "#;

    let mut headers = HashMap::new();
    headers.insert("Cookie", format!("session={}", session));

    let client = Client::new_with_headers(endpoint, headers);
    let data = client.query_unwrap::<Data>(query).await;

    if data.is_err() {
        return Ok(HttpResponse::Unauthorized().finish());
    }

    let vc = vc_registry
        .get_or_create_vc(&worker_manager, VcId("dreamh".into()))
        .await;

    let vc = match vc {
        Ok(vc) => vc,
        Err(error) => {
            eprintln!("{error}");

            return Ok(HttpResponse::NotFound().finish());
        }
    };

    match PeerConnection::new(vc, &data.unwrap().me.id).await {
        Ok(pc) => ws::start(pc, &request, stream),
        Err(error) => {
            eprintln!("{error}");

            Ok(HttpResponse::InternalServerError().finish())
        }
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();

    let worker_manager = actix_web::web::Data::new(WorkerManager::new());
    let vc_registry = actix_web::web::Data::new(VcRegistry::default());
    HttpServer::new(move || {
        App::new()
            .app_data(worker_manager.clone())
            .app_data(vc_registry.clone())
            .route("/ws", web::get().to(ws_index))
    })
    .bind("0.0.0.0:4002")?
    .run()
    .await
}
