pub mod cove {
    pub mod common {
        tonic::include_proto!("cove.common");
    }

    pub mod auth {
        tonic::include_proto!("cove.auth");
    }

    pub mod user {
        tonic::include_proto!("cove.user");
    }

    pub mod profile {
        tonic::include_proto!("cove.profile");
    }

    pub mod follow {
        tonic::include_proto!("cove.follow");
    }

    pub mod post {
        tonic::include_proto!("cove.post");
    }

    pub mod feed {
        tonic::include_proto!("cove.feed");
    }

    pub mod comment {
        tonic::include_proto!("cove.comment");
    }

    pub mod like {
        tonic::include_proto!("cove.like");
    }

    pub mod share {
        tonic::include_proto!("cove.share");
    }

    pub mod search {
        tonic::include_proto!("cove.search");
    }

    pub mod notification {
        tonic::include_proto!("cove.notification");
    }

    pub mod media {
        tonic::include_proto!("cove.media");
    }

    pub mod admin {
        tonic::include_proto!("cove.admin");
    }
}
