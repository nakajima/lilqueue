pub mod jobs {
    use sea_orm::entity::prelude::*;

    #[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
    #[sea_orm(table_name = "jobs")]
    pub struct Model {
        #[sea_orm(primary_key)]
        pub id: i64,
        pub job_type: String,
        pub payload: String,
        pub status: String,
        pub attempts: i32,
        pub max_attempts: i32,
        pub available_at: i64,
        pub locked_at: Option<i64>,
        pub lock_token: Option<String>,
        pub last_error: Option<String>,
        pub created_at: i64,
        pub updated_at: i64,
        pub completed_at: Option<i64>,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}
