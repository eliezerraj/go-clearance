# go-clearance

    workload for POC purpose

# tables

    CREATE TABLE public.clearance (
        id 				BIGSERIAL	NOT NULL,
        fk_order_id		BIGSERIAL	NOT NULL,
        type 			VARCHAR(100) NOT NULL,
        transaction_id	VARCHAR(100) NULL,
        status 			VARCHAR(100) NOT NULL,
        currency 		VARCHAR(100) NOT NULL,
        amount 			DECIMAL(10,2) NOT null DEFAULT 0,
        created_at 		timestamptz 	NOT NULL,
        updated_at 		timestamptz  NULL,
        CONSTRAINT clearance_pkey PRIMARY KEY (id)
    );

    ALTER TABLE clearance ADD constraint clearance_fk_order_id_fkey
    FOREIGN KEY (fk_order_id) REFERENCES public.order(id);