const { UPDATECOMPLIANTS,
	CREATEACCESS,
	NERDFIXTRANSPORT,
	UPDATENERDFIXPRODUCT,
	MAPPRODUCTREPORT,
	UPDATEPROJECTSQL,
	UPDATEESTIMATEVALUECOMPUTER,
	UPDATEESTIMATEVALUEMOBILE,
	CREATEPROJECTIFNEWONE
} = require('../../Functions');

module.exports = async function registerHook({ filter, action }, app) {
	const database = app.database;
	const cron = require("node-cron");
	const schema = await app.getSchema();
	const MailService = app.services.MailService;
	const mailService = new MailService({ schema });
	let moment = require('moment')
	let date = moment();
	const ItemsService = app.services.ItemsService;
	const assetsService = new ItemsService('Assets', {
		schema
	});
	const projectService = new ItemsService('project', {
		schema
	});
	const partnumberService = new ItemsService('part_numbers', {
		schema
	});

	const cronjobsservice = new ItemsService('cronjobs', {
		schema
	});

	const complaintsservice = new ItemsService('complaints', {
		schema
	});

	const usersservice = new ItemsService('directus_users', {
		schema
	});

	const nerdfixservice = new ItemsService('nerdfixs_history', {
		schema
	});

	const mail_historyservice = new ItemsService('mail_history', {
		schema
	});

	const partner_service = new ItemsService('partners', {
		schema
	});

	const deleted_asseets_service = new ItemsService('deleted_asseets', {
		schema
	});

	const pallet_service = new ItemsService('Pallets', {
		schema
	});

	const revisions_service = new ItemsService('directus_revisions', {
		schema
	});

	const estimate_values_service = new ItemsService('estimate_values_computer', {
		schema
	});

	const ServiceUnavailableException = app.exceptions.ServiceUnavailableException;
	const _ = require('lodash');

	const laptopData = ["CONVERTIBLE", "NOTEBOOK", "PORTABLE", "DETACHABLE"];
	const removeprocessorData = ["INTEL(R) PENTIUM(R) CPU ", "FAMILY: CORE I5; VERSION: ", "FAMILY: CORE I7; VERSION: ", "INTEL(R) XEON(R) CPU ", "INTEL(R) CORE(TM) ", "AMD ", "PROCESSOR ", "INTEL(R) XEON(R) CPU", "INTEL(R) CORE(TM)", "AMD", "PROCESSOR", "INTEL(R) XEON(R) CPU;", "INTEL(R) CORE(TM);", "AMD;", "PROCESSOR;", "11TH GEN ", "12TH GEN ", "13TH GEN ", "14TH GEN ", "15TH GEN ", "INTEL(R) XEON(R) ", "INTEL(R) CELERON(R) CPU ", "FAMILY: CELERON; VERSION: ", "INTEL(R) CELERON(R) ", "INTEL; FAMILY: XEON; VERSION: ", "INTEL(R) ATOM(TM) CPU "];
	const removeModelData = ["NOTEBOOK PC", "15.6 INCH", "14 INCH", "MOBILE WORKSTATION PC", "WITH RADEON GRAPHICS", "HP Compaq ", "HP ", "Dell ", "Lenovo ", "Compaq ", "HP Compaq", "HP", "Dell", "Lenovo", "Compaq"];
	const DesktopData = ["LOW PROFILE DESKTOP", "LUNCH BOX", "MICRO FORM FACTOR BTX", "MICROTOWER", "MINI DESKTOP", "MINI PC", "MINI TOWER", "SPACE-SAVING", "NUC", "SFF", "SLIM FORM FACTOR", "SMALL FORM FACTOR", "SMALL MINI TOWER", "TOWER", "ULTRA-SLIM DESKTOP PC"];
	function isNumber(value) {
		return typeof value === 'number';
	}

	filter("Assets.items.create", async function (input, { collection }, context) {

		console.log("---------------------");
		console.log("Assets Create Before Hook");
		console.log("---------------------");
		const { accountability } = context;
		let current_user = accountability
		try {
			// console.log("before creating", context);
			if (collection === "Assets") {
				if ((current_user?.role === 'ad406c8e-5746-4bb3-9fdb-aec086f992ae') || (input.data_generated === "CERTUS NL") || input.asset_id_nl) {
					const assetResult = await assetsService.readByQuery({
						fields: ["asset_id_nl"],
						filter: {
							asset_id_nl: {
								_eq: input.asset_id_nl
							}
						},
					});
					if (assetResult?.length > 0) {
						console.log("Asset created already");
						return
					}

					if (input.processor && input.processor.includes(" @ ")) {
						input.processor = input.processor.split(' @ ')[0]
					}
					let nerdfixTypes = input.asset_type ? input.asset_type.toLowerCase() : null;
					if (nerdfixTypes) {
						if (nerdfixTypes === "all-in one") {
							input.asset_type = 'COMPUTER'
						} else if (nerdfixTypes === "desktop") {
							input.asset_type = 'COMPUTER'
						} else if (nerdfixTypes === "mobile") {
							input.asset_type = 'MOBILE DEVICE'
						} else if (nerdfixTypes === "monitor") {
							input.asset_type = 'MONITOR'
						} else if (nerdfixTypes === "network") {
							input.asset_type = 'NETWORK'
						} else if (nerdfixTypes === "notebook") {
							input.asset_type = 'COMPUTER'
						} else if (nerdfixTypes === "server") {
							input.asset_type = 'SERVER & STORAGE'
						} else if (nerdfixTypes === "tablet") {
							input.asset_type = 'MOBILE DEVICE'
						} else if (nerdfixTypes === "tiny pc") {
							input.asset_type = 'COMPUTER'
						}
					}
					delete input.asset_id;
					// const projectdata = await projectService.readByQuery({
					// 	fields: ["id"],
					// 	filter: {
					// 		id: {
					// 			_eq: input.project_id
					// 		}
					// 	},
					// });
					// if (projectdata?.length === 0) {
					// 	delete input.project_id;
					// }
					input.data_generated = 'CERTUS NL';
					input.warehouse = 'NL01';
				}
				if (input.project_id) {
					input.project_id_1 = input.project_id
				}
				if (input.asset_id) {
					input.asset_id_1 = input.asset_id
				}

				if (input.asset_type) {
					input.asset_type = input.asset_type.toString().toUpperCase()
				}
				if (input.manufacturer && input.manufacturer.toUpperCase() === "HEWLETT-PACKARD") {
					input.manufacturer = "HP";
				}
				if (input.manufacturer) {
					input.manufacturer = input.manufacturer.toString()
					if (input.manufacturer && input.manufacturer.toUpperCase().includes('DELL')) {
						input.manufacturer = 'DELL'
					}
				}
				if (input.model) {
					input.model = input.model.toString()
				}
				if (input.processor) {
					input.processor = input.processor.toString()
				}
				if (input.data_generated) {
					input.data_generated = input.data_generated.toUpperCase()
				}
				if (input.sold_price) {
					input.sold_price = Math.round(input.sold_price)
				}
				if (input.complaint) {
					let complaint = input.complaint.toLowerCase()
					if (complaint.includes("hdd from server")) {
						input.asset_type = 'PARTS SERVER';
					}
					if (complaint.includes("hdd from pc")) {
						input.asset_type = 'PARTS COMPUTER';
					}
					if (complaint.includes("hdd from server") || complaint.includes("hdd from pc")) {
						input.form_factor = 'HDD';
						input.manufacturer = "";
						input.model = "";
						input.processor = "";
						input.memory = "";
						input.imei = "";
						input.graphic_card = "";
						input.serial_number = "";
						input.optical = "";
						input.battery = "";
						input.keyboard = "";
					}
				}
				if (input.form_factor) {
					let form_factor = input.form_factor.trim().toUpperCase();
					if (laptopData.includes(form_factor)) {
						input.form_factor = 'Laptop'
					} if (DesktopData.includes(form_factor)) {
						input.form_factor = 'Desktop'
					}
					//convert certus devices
					if (form_factor === 'RACK MOUNT CHASSIS') {
						input.asset_type = 'SERVER & STORAGE';
					}
				}
				if (input.status) {
					input.status = input.status.toUpperCase()
				}

				if (input.sold_price) {
					input.sold_price = Math.round(input.sold_price);
				}
				if (input.project_id && !isNaN(input.project_id)) {
					//check project exists if yes new project create
					await CREATEPROJECTIFNEWONE(input, projectService, ServiceUnavailableException);
				}
				if (input.processor) {
					removeprocessorData.forEach((removable) => {
						input.processor = input.processor.toUpperCase().replace(removable.toUpperCase(), "").trim();
					})
					input.processor = input.processor ? input.processor.toUpperCase() : '';
				}
				if (input.model) {
					removeModelData.forEach((removable) => {
						input.model = input.model.toUpperCase().replace(removable.toUpperCase(), "").trim();
					})
					let model = input.model.toUpperCase()
					if (model.includes('IMAC') || model.includes('AIO') || model.includes('ALL IN ONE')) {
						input.form_factor = 'All in One'
					}
				}
				if (input.manufacturer) {
					input.manufacturer = input.manufacturer ? input.manufacturer.toUpperCase() : ''
				}
				if (input.Part_No) {
					input.Part_No = isNumber(input.Part_No) ? (input.Part_No) : input.Part_No.toUpperCase().trim();
					let sql = `select action,part_no,status,model,asset_type,form_factor,manufacturer from public.part_numbers where part_no = '${input.Part_No}'`;
					await database.raw(sql)
						.then(async (response) => {
							if (input.processor) {
								removeprocessorData.forEach((removable) => {
									input.processor = input.processor.toUpperCase().replace(removable.toUpperCase(), "").trim();
								})
								input.processor = input.processor ? input.processor.toUpperCase() : ''
							}
							if (input.model) {
								removeModelData.forEach((removable) => {
									input.model = input.model.toUpperCase().replace(removable.toUpperCase(), "").trim();
								})
							}
							if (input.manufacturer) {
								input.manufacturer = input.manufacturer ? input.manufacturer.toUpperCase() : ''
							}
							if (response.rows.length === 0) {
								await partnumberService.createOne(
									{
										part_no: input.Part_No,
										status: 'draft',
										model: input.model || null,
										asset_type: input.asset_type || null,
										form_factor: input.form_factor || null,
										manufacturer: input.manufacturer || null,

									}
								).then((response1) => {
									// res.json(response);
									console.log("new part_number created =>", response1)
								}).catch((error1) => {
									console.log("new part numer failed =>", error1)
								});

								// let insertsql = `insert into public.part_numbers (part_no,status,model,asset_type,form_factor,manufacturer) values('${input.Part_No}', 'draft','${input.model || ''}','${input.asset_type || ''}','${input.form_factor || ''}','${input.manufacturer || ''}')`
								// await database.raw(insertsql);

							}
							let vals = response.rows[0] ? response.rows[0] : {};
							if (vals.status === 'published' && vals.model) {
								input.model = vals.model;
							}
							if (vals.status === 'published' && vals.asset_type) {
								input.asset_type = vals.asset_type;
							}
							if (vals.status === 'published' && vals.form_factor) {
								input.form_factor = vals.form_factor;
							}
							if (vals.status === 'published' && vals.manufacturer) {
								input.manufacturer = vals.manufacturer;
							}
						})
						.catch((error) => {
							// res.send(500)
						});
				}
				if (input.asset_type) {
					let query = ''
					if (input.form_factor) {
						query = `and TRIM(UPPER(formfactor))='${input.form_factor.toUpperCase()}'`
					} else {
						query = `and (formfactor is null OR formfactor = '')`
					}
					let sql = `select "Asset_Name",sampleco2,sample_weight,formfactor from public."AssetType" where UPPER("Asset_Name") like '${input.asset_type.toUpperCase()}' ${query} and "Asset_Name" is not null`;
					let assettypes = await database.raw(sql)
						.then(async (response) => {
							// console.log("response.rows", response.rows)
							if (response.rows && response.rows.length > 0) {
								return response.rows[0];
							} else {
								return null;
							}
						})
						.catch((error) => {
							// res.send(500)
						});
					if (assettypes) {
						input.sample_co2 = assettypes.sampleco2;
						input.sample_weight = assettypes.sample_weight;
					}
				}
				if (input.grade && input.grade.toUpperCase() === 'D') {
					let asset_type = input.asset_type.toUpperCase()
					if (asset_type === "COMPUTER" || asset_type === "SERVER & STORAGE") {
						input.status = 'HARVEST'
					}
				}
				input.quantity = input.quantity ? input.quantity : 1;
				// security update here
				// IF a computer or phone have data_destruction = “Erasure in progress” or data_destruction = “Not Erased”
				if (input.data_destruction && !input.asset_id_nl) {
					let dataDestruction = input.data_destruction.toLowerCase();
					if (
						dataDestruction === "erasure in progress"
						|| dataDestruction === "not erased/not erased/not erased"
						|| dataDestruction === "not erased/not erased"
						|| dataDestruction === "failed sectors"
						|| dataDestruction.includes("erased with warning(s) (reallocated sectors not erased: ")
						|| dataDestruction.includes("erased with warnings (reallocated sectors not erased: ")
						|| dataDestruction.includes("not erased (")
						|| dataDestruction.toLowerCase() === "not erased") {
						input.status = 'NOT ERASED';
					}
				}
				return input
			}

		} catch (error) {
			console.error(error);
		}
	});

	filter('items.create', async (input, { collection }, context) => {
		const { accountability } = context;
		let current_user = accountability

		// let current_user = accountability?.user
		if (collection === 'project_finance') {
			input.quantity = input.quantity ? input.quantity : 1
			if (input.quantity && input.price) {
				input.total = Number(input.quantity) * input.price
			}
		}
		if (collection === 'project') {

			if (!input.id) {
				let getsql = `select id from public."project" order by id desc limit 1`;
				await database.raw(getsql)
					.then(async (response) => {
						if (response.rows.length > 0) {
							input.id = response.rows[0].id + 1
						}
					})
					.catch((error) => {
						// res.send(500)
					});
			}
		}
		if (collection === 'client_assets') {
			let query = ''
			if (input.imei) {
				query = `and imei = '${input.imei}'`
			} else {
				query = `and (imei = '0' or imei = '' or imei is null)`
			}
			let getsql = `select asset_id from public."Assets" where serial_number = '${input.serial_number}' ${query}`;
			await database.raw(getsql)
				.then(async (response) => {
					if (response.rows.length > 0) {
						input.match = 'Match'
					} else {
						input.match = 'Not matched'
					}
				})
				.catch((error) => {
					// res.send(500)
				});
		}
		return input;
	});

	filter('items.update', async (input, { collection }, { schema }) => {
		if (collection === 'project_finance') {
			input.quantity = input.quantity ? input.quantity : 1
			if (input.quantity && input.price) {
				input.total = Number(input.quantity) * input.price
			}
		}
		if (collection === 'project') {

			// Set difference value
			let total = (parseFloat(input.remarketing || 0) - parseFloat(input.logistics || 0) - parseFloat(input.handling || 0) - parseFloat(input.software || 0) - parseFloat(input.other || 0));
			if (input.invoice_rec_amount > 0) {
				input.difference = parseFloat(total) - parseFloat(input.invoice_rec_amount);
			} else {
				input.difference = 0
			}
			if (input.project_type === 'PURCHASE') {
				input.commision_percentage = 0;
				input.order_commission = 0;
			}
			//set arrived time
			if (input.project_status === 'ARRIVED') {
				input.arrived_time = moment(new Date()).format('YYYY-MM-DDTHH:mm:00Z');
				input.order_commission = 0;
			}

			if (input.project_status === 'CLOSED' && (input.finish_date === null || input.finish_date === '')) {
				let finish_date = moment().format('YYYY-MM-DD');
				input.finish_date = finish_date;
			}
			if (input.id) {
				if (input.project_status === 'REPORTING') {
					let sql4 = `select count(*) cnt from public."Assets" where UPPER(asset_type) in ('MOBILE DEVICE', 'MONITOR', 'COMPUTER', 'MOBILE') and project_id = '${input.id}'`;
					await database.raw(sql4)
						.then(async (response) => {
							if (Number(response.rows[0].cnt) === 0) {
								//Changes in project status color
								input.status_color = response.rows[0].cnt;
							}
						})
						.catch((error) => {
							// res.send(500)
						});
				}
			}
		}
		if (collection === 'client_assets') {
			let query = ''
			if (input.imei) {
				query = `and imei = '${input.imei}'`
			} else {
				query = `and (imei = '0' or imei = '' or imei is null)`
			}
			// let getsql = `select asset_id from public."Assets" where serial_number = '${input.serial_number}' ${query}`;
			// await database.raw(getsql)
			// 	.then(async (response) => {
			// 		if (response.rows.length > 0) {
			// 			input.match = 'Match'
			// 		} else {
			// 			input.match = 'Not matched'
			// 		}
			// 	})
			// 	.catch((error) => {
			// 		// res.send(500)
			// 	});
		}
		if (collection === 'Assets' && input.asset_id) {
			//Checking previous project id
			delete input.user_updated;
			delete input.user_created;
			// --------------------
			// Here GHZ text removed from processor;
			if (input.processor && input.processor.includes(" @ ")) {
				input.processor = input.processor.split(' @ ')[0]
			}
			if (input.project_id) {
				input.project_id_1 = input.project_id
			}
			if (input.asset_id) {
				input.asset_id_1 = input.asset_id
			}
			if (input.asset_type) {
				input.asset_type = input.asset_type.toString().toUpperCase()
			}
			if (input.manufacturer && input.manufacturer.toUpperCase() === "HEWLETT-PACKARD") {
				input.manufacturer = "HP";
			}
			if (input.manufacturer) {
				input.manufacturer = input.manufacturer.toString();
				if (input.manufacturer && input.manufacturer.toUpperCase().includes('DELL')) {
					input.manufacturer = 'DELL'
				}
			}
			if (input.model) {
				input.model = input.model.toString()
			}
			if (input.processor) {
				input.processor = input.processor.toString()
			}
			if (input.data_generated) {
				input.data_generated = input.data_generated.toUpperCase()
			}
			if (input.sold_price) {
				input.sold_price = Math.round(input.sold_price)
			}
			if (input.complaint) {
				let complaint = input.complaint.toLowerCase()
				if (complaint.includes("hdd from server")) {
					input.asset_type = 'PARTS SERVER';
				}
				if (complaint.includes("hdd from pc")) {
					input.asset_type = 'PARTS COMPUTER';
				}
				if (complaint.includes("hdd from server") || complaint.includes("hdd from pc")) {
					input.form_factor = 'HDD';
					input.manufacturer = "";
					input.model = "";
					input.processor = "";
					input.memory = "";
					input.imei = "";
					input.graphic_card = "";
					input.serial_number = "";
					input.optical = "";
					input.battery = "";
					input.keyboard = "";
				}
			}
			if (input.form_factor) {
				let form_factor = input.form_factor.trim().toUpperCase();
				if (laptopData.includes(form_factor)) {
					input.form_factor = 'Laptop'
				} if (DesktopData.includes(form_factor)) {
					input.form_factor = 'Desktop'
				}
				//convert certus devices
				if (form_factor === 'RACK MOUNT CHASSIS') {
					input.asset_type = 'SERVER & STORAGE';
				}
			}
			if (input.status) {
				input.status = input.status.toUpperCase()
			}
			if (input.project_id) {
				const projectdata = await projectService.readByQuery({
					fields: ["id", "project_type"],
					filter: {
						id: {
							_eq: input.project_id
						}

					},
				});
				// console.log(input, "projectdata=>", projectdata)
				if (projectdata?.length > 0) {
					if (projectdata[0].project_type === 'ONHOLD') {
						if (input.status !== 'RETURNED' && input.status !== 'RECYCLED') {
							input.status = 'ON HOLD'
						}
					}
				} else if (projectdata?.length === 0) {
					//New project created
					await projectService.createOne(
						{
							id: input.project_id

						})
				}
			}
			if (input.processor) {
				removeprocessorData.forEach((removable) => {
					input.processor = input.processor.toUpperCase().replace(removable.toUpperCase(), "").trim();
				})
				input.processor = input.processor ? input.processor.toUpperCase() : ''
			}
			if (input.model) {
				removeModelData.forEach((removable) => {
					input.model = input.model.toUpperCase().replace(removable.toUpperCase(), "").trim();
				})
			}
			if (input.manufacturer) {
				input.manufacturer = input.manufacturer ? input.manufacturer.toUpperCase() : ''
			}
			if (input.sold_price) {
				input.sold_price = Math.round(input.sold_price);
			}
			if (input.Part_No) {
				input.Part_No = isNumber(input.Part_No) ? (input.Part_No) : input.Part_No.toUpperCase().trim();
				// let query = input.asset_type ? `and UPPER(asset_type) = '${input.asset_type.toUpperCase()}'` : ''
				let sql = `select action,part_no,status,model,asset_type,form_factor,manufacturer from public.part_numbers where part_no = '${input.Part_No}'`;
				// let sql = `select action,part_no,status,model,asset_type,form_factor,manufacturer from public.part_numbers where part_no = '${input.Part_No}'`;
				await database.raw(sql)
					.then(async (response) => {
						if (response.rows.length === 0) {
							await partnumberService.createOne(
								{
									part_no: input.Part_No,
									status: 'draft',
									model: input.model || null,
									asset_type: input.asset_type || null,
									form_factor: input.form_factor || null,
									manufacturer: input.manufacturer || null,

								}
							).then((response1) => {
								// res.json(response);
								console.log("new part_number created =>", response1)
							}).catch((error1) => {
								console.log("new part numer failed =>", error1)
							});
							// let insertsql = `insert into public.part_numbers (part_no,status,model,asset_type,form_factor,manufacturer) values('${input.Part_No}', 'draft','${input.model || null}','${input.asset_type || null}','${input.form_factor || null}','${input.manufacturer || null}')`
							// await database.raw(insertsql);
						}
						if (input.processor) {
							removeprocessorData.forEach((removable) => {
								input.processor = input.processor.toUpperCase().replace(removable.toUpperCase(), "").trim();
							})
							input.processor = input.processor ? input.processor.toUpperCase() : ''
						}
						if (input.model) {
							removeModelData.forEach((removable) => {
								input.model = input.model.toUpperCase().replace(removable.toUpperCase(), "").trim();
							})
						}
						if (input.manufacturer) {
							input.manufacturer = input.manufacturer ? input.manufacturer.toUpperCase() : ''
						}
						let vals = response.rows[0] ? response.rows[0] : {};
						if (vals.status === 'published' && vals.model) {
							input.model = vals.model;
						}
						if (vals.status === 'published' && vals.asset_type) {
							input.asset_type = vals.asset_type;
						}
						if (vals.status === 'published' && vals.form_factor) {
							input.form_factor = vals.form_factor;
						}
						if (vals.status === 'published' && vals.manufacturer) {
							input.manufacturer = vals.manufacturer;
						}
					})
					.catch((error) => {
						// res.send(500)
					});
			}

			if (input.asset_type && !input.part_number_update) {
				let query = ''
				if (input.form_factor) {
					query = `and TRIM(UPPER(formfactor))='${input.form_factor.toUpperCase()}'`
				} else {
					query = `and (formfactor is null OR formfactor = '')`
				}
				let sql = `select "Asset_Name",sampleco2,sample_weight,formfactor from public."AssetType" where UPPER("Asset_Name") = '${input.asset_type.toUpperCase()}' ${query} and "Asset_Name" is not null`;
				let assettypes = await database.raw(sql)
					.then(async (response) => {
						// console.log("response.rows", response.rows)
						if (response.rows && response.rows.length > 0) {
							return response.rows[0];
						} else {
							return null;
						}
					})
					.catch((error) => {
						// res.send(500)
					});
				// console.log("assettypes", assettypes)
				if (assettypes) {
					input.sample_co2 = assettypes.sampleco2;
					input.sample_weight = assettypes.sample_weight;
				}
			}

			//Add a sold price per unit in the system
			if (input.sold_price) {
				let qty_sold = 0
				if (input.qty_sold) {
					qty_sold = input.qty_sold
				}
				if ((Number(input.qty_sold) === 0) && input.quantity) {
					qty_sold = input.quantity
				}
				input.sold_price_total = Number(qty_sold) * Number(input.sold_price);
			}
			//Update count on complaints
			if (input.platform === 'MOBILE_UPDATE' || input.platform === 'MOBILE_UPDATE_CERTUS') {
				if (input.complaint && (input.complaint.toLowerCase().includes("touchscreen;min"))) {
					input.screen = 'Touchscreen min';
					input.complaint = input.complaint.toLowerCase().replace(',touchscreen;min', "").trim();
					input.complaint = input.complaint.toLowerCase().replace('touchscreen;min', "").trim();
				}
				await UPDATECOMPLIANTS(input, complaintsservice, database)
			}
			// security update here
			// IF a computer or phone have data_destruction = “Erasure in progress” or data_destruction = “Not Erased”
			if (input.data_destruction && !input.asset_id_nl) {
				let dataDestruction = input.data_destruction.toLowerCase();
				if (
					dataDestruction === "erasure in progress"
					|| dataDestruction === "not erased/not erased/not erased"
					|| dataDestruction === "not erased/not erased"
					|| dataDestruction === "failed sectors"
					|| dataDestruction.includes("erased with warning(s) (reallocated sectors not erased: ")
					|| dataDestruction.includes("erased with warnings (reallocated sectors not erased: ")
					|| dataDestruction.includes("not erased (")
					|| dataDestruction.toLowerCase() === "not erased") {
					input.status = 'NOT ERASED';
				}
			}
			// update grade DV or EV should automatically be added as ONHOLD.
			const revisionsData = await revisions_service.readByQuery({
				fields: ["item", "data"],
				limit: 1,
				sort: ['-activity'],
				filter: {
					collection: {
						_contains: 'Asset'
					},
					item: {
						_eq: input.asset_id
					}

				},
			});
			if (revisionsData.length > 0 && input.grade
				&& (input.grade !== 'EV' || input.grade !== 'DV')
				&& (revisionsData[0]?.data?.status !== 'RESERVATION' && revisionsData[0]?.data?.status !== 'SOLD')
				&& (revisionsData[0]?.data?.grade === 'DV' || revisionsData[0]?.data?.grade === 'EV')
			) {
				if (input.grade === 'E') {
					input.status = 'RECYCLED'
				} else {
					input.status = 'IN STOCK'
				}
			} if (input.grade && !revisionsData[0]?.data?.grade && (input.grade.toUpperCase().includes('V'))) {
				input.status = 'ON HOLD'

			} if (revisionsData.length > 0 && input.grade
				&& (input.grade.toUpperCase().includes('V'))
				&& input.grade && (revisionsData[0]?.data?.status !== 'RESERVATION' && revisionsData[0]?.data?.status !== 'SOLD')) {
				input.status = 'ON HOLD';
			}
			if (revisionsData.length > 0 && input.grade
				&& (!input.grade.toUpperCase().includes('V'))
				&& (input.grade !== 'EV' || input.grade !== 'DV')
				&& (revisionsData[0]?.data?.status !== 'RESERVATION' && revisionsData[0]?.data?.status !== 'SOLD')
				&& (revisionsData[0]?.data?.grade === 'DV' || revisionsData[0]?.data?.grade === 'EV')
			) {
				if (input.grade === 'E') {
					input.status = 'RECYCLED'
				} else {
					input.status = 'IN STOCK'
				}
			}
			return input
		};
		return input;
	});

	action('items.delete', async (input) => {
		if (input.collection === 'Assets') {
			let deletedAssetIds = input.payload;
			const deletedAssets = await deleted_asseets_service.readByQuery({
				fields: ["project_id"],
				limit: -1,
				filter: {
					asset_id: {
						_in: deletedAssetIds
					}
				}
			});

			if (deletedAssets.length > 0) {
				let projIds = deletedAssets.map(
					(item) => item.project_id
				);
				projIds = _.uniq(projIds);
				projIds.forEach(async (obj) => {
					await UPDATEPROJECTSQL(obj, database)
					//----------- update processed unit sold
				})
			}
		}
		// return input;

	});

	action('items.update', async (input) => {
		let data = input.payload;
		if (input.collection === 'Assets') {
			if (data.asset_id_nl && data.status === 'RESERVATION' && !data.nerdfix_update && data.manualNerdFixUpdate) {
				await NERDFIXTRANSPORT(data, assetsService, nerdfixservice, database, date, ServiceUnavailableException);
			}
			if (data.asset_id_nl && !data.nerdfix_update) {
				await UPDATENERDFIXPRODUCT(data, assetsService, ServiceUnavailableException, database, 'UPDATEPRODUCT_API', nerdfixservice);
			}

			//update project id and asset id
			// await updateAssetIdProjectId(data);
			//end
			if (data.pallet_number) {
				let sql = `select count(*) cnt from public."Assets" where pallet_number = '${data.pallet_number}'`;
				await database.raw(sql)
					.then(async (response) => {
						let count = response.rows[0].cnt
						let sql = `update public."Pallets" set pallet_items = '${count}' where pallet_number = '${data.pallet_number}'`;
						await database.raw(sql)
					})
					.catch((error) => {
						// res.send(500)
					});
			}
			// Update asset status if pallet as In Production
			await UpdatePalletStatus(data, 'assets')

			if (data.asset_id) {
				await updateProject(data, 'update')
			}

			//update client asset matched assetes
			let query = ''
			if (data.imei) {
				query = `and imei = '${data.imei}'`
			} else {
				query = `and (imei = '0' or imei = '' or imei is null)`
			}
			// let getsql = `select asset_id from public."Assets" where serial_number = '${data.serial_number}' ${query}`;
			// await database.raw(getsql)
			// 	.then(async (response) => {
			// 		if (response.rows.length > 0) {
			// 			let updateprojectsql = `UPDATE public.client_assets a SET match = 'Match'  where serial_number = '${data.serial_number}' ${query}`;
			// 			await database.raw(updateprojectsql)
			// 		}
			// 	})
			// 	.catch((error) => {
			// 		// res.send(500)
			// 	});

			//Update techvaluator page by computer
			// const assetResult = await assetsService.readByQuery({
			// 	fields: ["processor", "asset_type", "hdd", "model"],
			// 	filter: {
			// 		asset_id: {
			// 			_eq: data.asset_id
			// 		}
			// 	},
			// });
			// if (assetResult?.length > 0 && assetResult[0].asset_type === 'COMPUTER') {
			// 	await UPDATEESTIMATEVALUECOMPUTER(assetResult[0], database, estimate_values_service)
			// } if (assetResult?.length > 0 && assetResult[0].asset_type.includes('MOBILE')) {
			// 	await UPDATEESTIMATEVALUEMOBILE(assetResult[0], database, estimate_values_service)
			// }
		};
		if (input.collection === 'project' && data.id) {
			if (data.project_status !== 'CLOSED' || data.project_type === 'PURCHASE') {
				await updateProjectFinance(data.id)
			}
			if (data.id && data.project_status === "PROCESSING FINISHED") {
				await sendMail('processed_finished', data, data.id)
			}
			if (data.id && data.project_status === "ARRIVED") {
				await sendMail('arrived', data, data.id)
			}
			// await CREATEACCESS(data, projectService, usersservice)
			// Asset status will update to ON HOLD once project_type set to ON HOLD
			if ((data.project_type === 'ONHOLD') && data.project_status !== 'CLOSED') {
				let obj = { status: 'ON HOLD' };
				if (data.project_type === 'ITAD') {
					obj.status = 'IN STOCK'
				}
				const assetData = await assetsService.readByQuery({
					fields: ["asset_id"],
					filter: {
						project_id: {
							_eq: data.id
						},
						status: {
							_nin: ['SOLD', 'RESERVATION'],
						}
					},
				});
				if (assetData?.length > 0) {
					let assetIds = assetData.map(
						(item) => item.asset_id
					);
					return await assetsService.updateMany(assetIds,
						obj
					).then((response1) => {
						// res.json(response);
						console.log(`update assets if project type ${obj.status} ==>`, response1)
					}).catch((error1) => {
						console.log(`update assets if project type ${obj.status} failed ==>`, error1)
					});
				}
			}
		};

		if (input.collection === 'project_finance') {
			if (data.type) {
				data.type = data.type.toLowerCase()
			}
			await updateProjectFinance(data.project_id)
		}
		if (input.collection === 'part_numbers') {
			await update_part_number_withassets(input.keys[0])
		}
	});
	async function update_part_number_withassets(id) {
		try {
			const partnumber = await partnumberService.readByQuery({
				fields: ["action", "part_no", "status", "model", "asset_type", "form_factor", "manufacturer", 'co2', 'weight'],
				filter: {
					id: {
						_eq: id
					}

				},
			});


			if (partnumber?.length > 0) {
				let fields = partnumber[0];
				if (fields.status === 'published') {
					const assetList = await assetsService.readByQuery({
						fields: ["asset_id", "model", "asset_type", "form_factor", "manufacturer", "Part_No"],
						filter: {
							_or: [
								{ "Part_No": { _icontains: 'N/A' } },
								{ "Part_No": { _nnull: true } },
							],
							_and: [
								{ "Part_No": { _icontains: `${fields.part_no}` } }
							]
						},
						limit: -1
					});

					if (assetList?.length > 0) {
						// let selectedAssetIds = assetList.map(
						// 	(key) => key.asset_id
						// );
						assetList.forEach(async (item) => {
							let obj = {
								model: fields.model,
								asset_type: fields.asset_type,
								manufacturer: fields.manufacturer,
								form_factor: fields.form_factor,
								asset_id: item.asset_id,
								sample_co2: fields.co2,
								sample_weight: fields.weight,
								part_number_update: 'true'
							}
							// console.log("objjj", obj)
							// return
							return await assetsService.updateOne(item.asset_id,
								obj
							).then((response1) => {
								// res.json(response);
								console.log("asset part number success", response1)

							}).catch((error1) => {
								console.log("partnumer asset update", error1)
							});
						});
					}
				}
			}
		} catch (error) {
			console.log("error on part no update", error)
			throw new ServiceUnavailableException(error);
		}

	}
	async function updateAssetIdProjectId(data) {
		let obj = {};
		if (data.asset_id) {
			if (data.project_id) {
				obj.project_id_1 = data.project_id
			}
			if (data.asset_id) {
				obj.asset_id_1 = data.asset_id
			}
			await assetsService.updateOne(data.asset_id,
				obj
			).then((response1) => {
				// res.json(response);
				console.log("asset project update success => ", response1)
			}).catch((error1) => {
				// console.log("dataaaaaaa", data.asset)

				console.log("asset id update errrrr=>", error1)
			});
		}
	}

	action('items.create', async (input) => {
		let data = input.payload;
		if (input.collection === 'Assets') {
			if (!data.asset_id) {
				await updateAssetIdProjectId(data);
			}
			if (data.pallet_number) {
				let sql = `select count(*) cnt from public."Assets" where pallet_number = '${data.pallet_number}'`;
				await database.raw(sql)
					.then(async (response) => {
						let count = response.rows[0].cnt

						let sql = `update public."Assets" set pallet_items = '${count}',date_updated = '${date.format('YYYY-MM-DD')}' where pallet_number = '${data.pallet_number}'`;
						await database.raw(sql)
					})
					.catch((error) => {
						// res.send(500)
					});
			}
			if (data.asset_id) {
				updateProject(data, 'create')
			}
			//Update count on complaints
			if (data.platform === 'MOBILE_UPDATE' || data.platform === 'MOBILE_UPDATE_CERTUS') {
				await UPDATECOMPLIANTS(data, complaintsservice, database)
			}
		};
		if (input.collection === 'project') {
			if (data.id) {
				await sendMail('create', data, data.id)
				await CREATEACCESS(data, projectService, usersservice);
				await updatePartnerCommission(data)
			}
		};
		if (input.collection === 'project_finance') {
			if (data.type) {
				data.type = data.type.toLowerCase()
			}
			await updateProjectFinance(data.project_id)
		}
		if (input.collection === 'project') {
			await updateProjectFinance(data.id)
		}
	});

	async function updateProjectFinance(project_id) {
		project_id = typeof project_id === 'object' ? project_id.id : project_id;
		if (!project_id) {
			return
		}
		const projectdata = await projectService.readByQuery({
			fields: ["project_type", "kickback_percentage", "kickback_revenue", "order_commission", "order_revenue", "commision_percentage", "revenue", "remarketing", "software", "handling", "logistics", "buyout", "other", "invoice_by_client", "invoice_rec_amount"],
			filter: {
				id: {
					_eq: project_id
				},
				project_status: {
					_neq: 'CLOSED'
				},
				status: {
					_neq: 'archived'
				}
			},
		});
		if (projectdata?.length > 0) {
			let projectValues = projectdata[0];
			let values1 = {};
			let types = ["remarketing", "logistics", "handling", "software", "buyout", "other"];
			let sql1 = `select lower(type) as type,sum(total) as total from public."project_finance" where project_id = ${project_id} and status !='archived' group by lower(type)`;
			await database.raw(sql1)
				.then(async (response1) => {
					if (response1.rows?.length > 0) {
						let result = response1.rows;
						// console.log("result", result)
						result.forEach((item, i) => {
							if (types.includes(item.type)) {
								values1[item.type] = item.total
							}
						})
					}
				})
			let remarketing = 0;
			let buyout = values1?.buyout ? Number(values1.buyout) : 0
			let logistics = values1?.logistics ? Number(values1.logistics) : 0
			let handling = values1?.handling ? Number(values1.handling) : 0
			let other = values1?.other ? Number(values1.other) : 0
			let software = projectValues.software || 0;
			let commision_percentage = projectValues.commision_percentage;
			let kickback_percentage = projectValues.kickback_percentage || 0;

			let order_commission = 0
			if (Number(projectValues.order_commission)) {
				order_commission = projectValues.order_commission
			} else if (!projectValues.commision_percentage && projectValues.order_commission) {
				order_commission = projectValues.order_commission;
			} else if (!projectValues.commision_percentage && !projectValues.order_commission) {
				order_commission = projectValues.order_commission = 15
			}

			//Set order_commission is 0 if project type as purchase

			if (projectValues.project_type === 'PURCHASE') {
				order_commission = projectValues.order_commission = 0;
			}
			let order_revenue = 0
			if (projectValues.order_revenue) {
				order_revenue = parseFloat(projectValues.order_revenue);
			}
			let revenue = 0
			if (projectValues.revenue) {
				revenue = parseFloat(projectValues.revenue);
			}
			let order_commission_value = 0
			if (order_revenue && order_commission) {
				order_commission_value = Math.round((order_revenue / 100) * order_commission)
			}
			let commision = Math.round((projectValues.revenue / 100) * commision_percentage) + order_commission_value;
			// remarketing = Math.round(projectValues.revenue - commision);
			let kickback_revenue = kickback_percentage ? parseInt(kickback_percentage) * ((parseInt(revenue) + parseInt(order_revenue)) / 100) : 0

			remarketing = Math.round(parseFloat(projectValues.revenue) + parseFloat(order_revenue) + parseFloat(buyout) - commision - kickback_revenue);

			let invoice_by_client = 0;
			// console.log("**************", values)
			invoice_by_client = parseFloat(remarketing) - parseFloat(logistics) - parseFloat(handling) - parseFloat(software) - parseFloat(other);
			// console.log("invoice_by_client",invoice_by_client)
			let difference = 0;
			if (projectValues.invoice_rec_amount) {
				difference = parseFloat(invoice_by_client) - parseFloat(projectValues.invoice_rec_amount);
			}
			let obj = {
				logistics: logistics,
				handling: handling,
				buyout: buyout,
				other: other,
				software: software,
				invoice_by_client: invoice_by_client || 0,
				remarketing: remarketing || 0,
				order_commission: order_commission,
				commision: commision,
				difference: difference,
				kickback_revenue: Math.round(kickback_revenue)
			}
			// console.log("===>", obj)

			return await projectService.updateOne(project_id,
				obj
			).then((response1) => {
				// res.json(response);
				console.log("project finance updateee success", response1)
			}).catch((error1) => {
				console.log("project finance updateee errrrr", field.asset_id)
			});


		}
	}
	async function updateProject(data, actionpage) {
		if (data.asset_id) {
			let getsql = `select project_id,status,date_nor from public."Assets" where asset_id = '${data.asset_id}'`;
			await database.raw(getsql)
				.then(async (response) => {
					data.project_id = response.rows[0].project_id;
					let status = response.rows[0].status;
					//---------------------------------- update date nor automatically 
					if (status === 'RESERVATION') {
						let updatedatenor = `update public."Assets" set date_nor = '${moment().format('YYYY-MM-DD')}' where asset_id = ${data.asset_id} and status = 'RESERVATION' and (date_nor is null)`;
						await database.raw(updatedatenor)
					}
					//-----------------------------
					//update no_of_assets,no_of_assets_1 and processed_units_sold value
					if (data.project_id) {
						await UPDATEPROJECTSQL(data.project_id, database);
					}

					if (actionpage === 'create' || status === 'IN STOCK' || status === 'NOT ERASED' || status === 'TEMP') {
						await callTargetPrice(data, true)
					}

					//Processing of equipment in Itreon project  has been initiated
					await sendMail('process_started', data, data.project_id)
					//------end-----------
					// send mail if first asset sold in the project

					const assetResult = await assetsService.readByQuery({
						fields: ["asset_id", "status", "date_created"],
						sort: ['-date_created'],
						filter: {
							_and: [
								{
									project_id: { _eq: data.project_id }
								}
							]
						}
					});
					if (assetResult?.length > 0) {
						//------------ send mail if first asset sold in the project
						let soldAssets = assetResult.filter((asset) => (asset?.status?.toUpperCase() === 'SOLD') || (asset?.status?.toUpperCase() === 'RESERVATION'));
						//---------end send mail if first asset sold in the project
						const projcets = await projectService.readByQuery({
							fields: ["process_start_date", "id", "project_status", "finish_date"],
							filter: {
								id: {
									_eq: data.project_id,
								}
							},
						});
						if (soldAssets?.length === 1 && projcets && projcets[0].project_status !== 'CLOSED') {
							await sendMail('sold', data, data.project_id);
						}
						if (projcets && assetResult?.length > 0) {
							//set project status as PROCESSING
							let date_created = assetResult[0].date_created;
							if (date_created && !projcets[0].process_start_date) {
								let obj = {};
								if (projcets[0].project_status === 'ORDER' || projcets[0].project_status === 'ARRIVED') {
									obj.project_status = 'PROCESSING';
								}
								if (projcets[0].project_status === 'PROCESSING') {
									delete obj.project_status;
								}
								const date = moment(date_created);
								obj.process_start_date = date.format('YYYY-MM-DD');
								await projectService.updateOne(data.project_id,
									obj
								).then(async (response1) => {
									console.log("Set project status as PROCESSING success", data.project_id);
								}).catch((error1) => {
									console.log("Set project status as PROCESSING error", data.project_id);
								});
							}

						}
					}

				})
				.catch((error) => {
					// res.send(500)
				});

		}
	}

	action("files.upload", async function (eventObj, context) {
		let payload = eventObj.payload;
		if (payload.asset_id) {
			await assetsService.updateOne(payload.asset_id, {
				total_file_upload: 1
			}).then(async (response1) => {
				// res.json(response);
				console.log("asset images update success", response1)
			}).catch((error1) => {
				console.log("asset images update fail", error1)
			});
		} if (payload.project_id) {
			await projectService.updateOne(payload.project_id, {
				total_images: 1
			}).then(async (response1) => {
				// res.json(response);
				console.log("project images update success", response1)
			}).catch((error1) => {
				console.log(" project images update fail", error1)
			});
		}
	});

	action("Pallets.items.update", async function (eventObj, context) {
		console.log("---------------------");
		console.log("Pallets update After Hook");
		console.log("---------------------");
		if (eventObj?.payload?.pallet_type.toUpperCase() === 'MONITOR') {
			UpdatePalletStatus(eventObj.payload);
		}
	});

	async function UpdatePalletStatus(payload, collection = null) {
		try {
			if (!payload.pallet_number)
				return
			//--------------------
			let obj = {};
			const pallet_detail = await pallet_service.readByQuery({
				fields: [
					"pallet_status", "pallet_type"
				],
				filter: {
					pallet_number: {
						_eq: payload.pallet_number
					}
				},
			});
			if (pallet_detail?.length > 0 && pallet_detail[0] && pallet_detail[0]?.pallet_type.toUpperCase() === 'MONITOR') {
				let palletDetail = pallet_detail[0];
				if (palletDetail?.pallet_status.toUpperCase() === 'IN PRODUCTION') {
					obj.status = 'ON HOLD';
				}
				if (palletDetail?.pallet_status.toUpperCase() === 'FOR SALE') {
					obj.status = 'IN STOCK';
				}
				if (obj.status) {
					const asset_lists = await assetsService.readByQuery({
						fields: [
							"asset_id"
						],
						filter: {
							pallet_number: {
								_eq: payload.pallet_number
							},
							status: {
								_nin: ['SOLD', 'RESERVATION'],
							}
						},
					});
					let assetIdsList = asset_lists.map(
						(item) => item.asset_id
					);

					return await assetsService.updateMany(assetIdsList,
						obj
					).then((response1) => {
						// res.json(response);
						console.log("update assets if pallet status on For sale ==>", response1)
					}).catch((error1) => {
						console.log("update assets if pallet status on For sale failed ==>", error1)
					});
				}


			}
		} catch (error) {
			console.error(error);
		}
	}

	//0 0 * * SUN
	//*/2 * * * *
	//0 19,20,21 * * *
	cron.schedule('0 19,20,21 * * *', async () => {
		await cronjobsservice.createOne(
			{
				date: moment().format('YYYY-MM-DD'),
				type: 'targetprice'
			}
		).then(async (response1) => {
			// res.json(response);
			await callTargetPrice(null, false);
			console.log("cron job create success", response1)
		}).catch((error1) => {
			console.log("cron job create errrrr", error1)
		});
	});
	// cron.schedule('*/1 * * * *', async () => {
	// 	await fetchMDMLockedDevices()

	// });
	// await fetchMDMLockedDevices()

	// async function fetchMDMLockedDevices() {
	// 	try {
	// 		let one_day_before = moment().subtract(1, 'days').format('YYYY-MM-DD');
	// 		const mdmlockedDevices = await assetsService.readByQuery({
	// 			fields: ["project_id.id", "project_id.client.client_name", "complaint", "model", "serial_number", "imei", "project_id.contact_attn.email"],
	// 			limit: -1,
	// 			filter: {
	// 				_and: [
	// 					// {
	// 					// 	"project_id": {
	// 					// 		"id": {
	// 					// 			_eq: 60001899
	// 					// 		}
	// 					// 	}
	// 					// },
	// 					{
	// 						"project_id": {
	// 							"client": {
	// 								_nnull: true
	// 							}
	// 						}
	// 					},
	// 					{
	// 						"project_id": {
	// 							"project_status": {
	// 								_neq: 'CLOSED'
	// 							}
	// 						}
	// 					},
	// 					{
	// 						"project_id": {
	// 							"date_created": {
	// 								_lte: one_day_before
	// 							}
	// 						}
	// 					},
	// 				],
	// 				_or: [
	// 					{ complaint: { _icontains: 'BIOS' } },
	// 					{ complaint: { _icontains: 'MDM lock' } },
	// 					{ complaint: { _icontains: 'apple id lock' } },
	// 					{ complaint: { _icontains: 'Knox lock' } },
	// 					{ complaint: { _icontains: 'iCloud lock' } },
	// 					{ complaint: { _icontains: 'computrace' } },
	// 					{ complaint: { _icontains: 'iCloud / Knox lock' } }
	// 				]
	// 			}
	// 		});
	// 		// console.log("mdmlockedDevices ===>", mdmlockedDevices)

	// 		if (mdmlockedDevices?.length > 0) {
	// 			let lockedDevices = _.groupBy(mdmlockedDevices, (item) => item.project_id.id);

	// 			Object.keys(lockedDevices).forEach(async function (key) {
	// 				let values = lockedDevices[key];
	// 				await SENDMAILLOCKEDDEVICES(database, key, mailService, values)
	// 			});
	// 		}



	// 	} catch (err) {
	// 		console.log("mdm locked device errr ==>", err)
	// 		throw err
	// 	}
	// }

	//excel export and send mail


	// await sendMail('create', null, '111')
	async function sendMail(action, data, project_id) {
		try {
			let emails = [];
			let body = ``;
			let html = ``
			let subject = ``;
			let content = ``
			// let delivery_address = '';
			let suborg_name = '';
			let client_name = '';
			let client_ref = '';
			let mail_status = ''
			let arrived_time = ''
			let project_name = ''
			if (project_id) {
				const projectdata = await projectService.readByQuery({
					fields: ["id", "delivery_address.delievery_address", "delivery_address.sub_org", "client.client_name", "client_ref", "tk_trigger.project_users_id.email", "arrived_time", "project_name"],
					filter: {
						id: {
							_eq: project_id
						}
					},
				});
				project_name = projectdata[0]?.project_name || '';
				client_name = projectdata[0]?.client?.client_name || '';
				client_ref = projectdata[0]?.client_ref || '';
				delivery_address = projectdata[0]?.delivery_address || '';
				suborg_name = projectdata[0]?.sub_org || '';
				arrived_time = `(${projectdata[0]?.arrived_time})` || '';
				let tk_trigger = projectdata[0].tk_trigger;
				emails = tk_trigger.map(
					(item) => item.project_users_id.email
				);
				// emails = ["anandsdn@gmail.com"]
				if (action === 'create') {
					mail_status = 0;
				} if (action === 'process_started') {
					mail_status = 1;
				} if (action === 'sold') {
					mail_status = 2;
				} if (action === 'processed_finished') {
					mail_status = 3;
				} if (action === 'arrived') {
					mail_status = 4;
				}
				const mail_history = await mail_historyservice.readByQuery({
					fields: ["id"],
					filter: {
						project_id: {
							_eq: project_id
						},
						mail_status: {
							_eq: `${mail_status}`
						}
					},
				});
				if (projectdata?.length > 0 && emails?.length > 0 && mail_history?.length > 0) {
					return false
				}
				// email content
				let footer = `<span>Best regards</span><br/><span>Itreon</span><br/><span>support@itreon.se</span>`
				if (action === 'create') {
					mail_status = 0;
					subject = `New Itreon project created - ${project_id}`
					body = `<span>Hi,</span><br/<br/>We have created a new project with id - <strong>${project_id}</strong>.`
					html = `<table style="font-size: 14px;width:100%"><tr>${body}<br/></tr>${client_name ? '<tr><td style="font-weight: 500;">Client name: <span ><strong>' + client_name + '</strong></span></td></tr>' : ''}${project_name ? '<tr><td style="font-weight: 500;">Project name: <span ><strong>' + project_name + '</strong></span></td></tr>' : ''}<tr><td><tr><td>Please already now check assets for locks and provide Itreon with any needed BIOS password. Note that unlocked assets are processed quicker and to a lower cost than assets that need to be unlocked and reprocessed. In worst case a asset with locks will be unusable.</td></tr><tr><td><tr>For more information about making a IT takeback go to https://www.itreon.eu/client-information</td></tr><td>If something is incorrect or if you have any other questions please contact us immediately.</td></tr></table >`
					content = `${html}`
				}
				if (action === 'process_started') {
					mail_status = 1;
					subject = `Processing of equipment in Itreon project ${project_id} has been initiated`
					body = `<table style="font-size: 14px;width:100%"><tr><td><div><span>Hi,</span><br/<br/>We want to inform you that the processing of your equipment in project <strong>${project_id}</strong> has been initiated. There is nothing you need to do. We will keep you updated and informed along the way. </div></td></tr><tr><td>Please do not hesitate to contact us in case you have any questions.</td></tr></table>`;
					html = `${body}`
					content = `${html}`
				}
				if (action === 'sold') {
					mail_status = 2;
					subject = `Selling of equipment in Itreon project ${project_id} has begun`
					body = `<table style="font-size: 14px;width:100%"><tr><td><div ><span>Hi,</span><br/><br/>We are happy to inform you that the selling of the equipment in project <strong>${project_id}</strong> for client <strong>${client_name}</strong> has begun. We will contact you again once we are done.</div></td></tr><tr><td>Please do not hesitate to contact us in case you have any other questions.</td></tr></table>`;
					html = `${body}`
					content = `${html}`
				}
				if (action === 'processed_finished') {
					mail_status = 3;
					subject = `Processing finished in Itreon project - ${project_id}`
					body = `<table style="font-size: 14px;width:100%"><tr><td><div><span>Hi,</span><br/><br/>We have now processed all equipment in project <strong>${project_id}</strong> for client <strong>${client_name}</strong>.<br/><br/>You can now access our system to get your product report.</div></td></tr><tr><td><br/>Let us know if you have any other questions.</td></tr></table>`;
					html = `${body}`
					content = `${html}`

					const asset_lists = await assetsService.readByQuery({
						fields: [
							"asset_id", "project_id", "asset_type",
							"form_factor", "quantity", "manufacturer", "model",
							"imei", "serial_number", "processor", "memory", "hdd",
							"grade", "complaint_1", "complaint", "optical", "graphic_card",
							"battery", "keyboard", "screen", "grade", "erasure_ended",
							"data_destruction", "wipe_standard",
							"hdd_serial_number", "erasure_ended", "data_destruction", "wipe_standard",
							"sample_co2", "sample_weight", "Part_No", "battery", "keyboard", "screen"
						],
						filter: {
							project_id: {
								_eq: project_id
							},
							asset_status: {
								_eq: 'not_archived'
							}
						},
					});
					// console.log("asset_lists", asset_lists)
					let map_product_report = '';
					const filename = `product report - ${project_id}.xlsx`;
					if (asset_lists?.length > 0) {
						asset_lists.forEach((obj) => {
							obj.client_ref = client_ref;
							obj.client_name = client_name;
							obj.suborg_name = suborg_name;
						})
						map_product_report = await MAPPRODUCTREPORT(asset_lists)
						// emails = ["anandsdn@gmail.com"]
						// emails.forEach(async (email) => {
						// 	try {
						// 		await mailService.send({
						// 			to: [email],
						// 			subject: subject,
						// 			html: content,
						// 			attachments: [
						// 				{
						// 					filename,
						// 					content: map_product_report,
						// 					contentType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
						// 				},
						// 			],
						// 		}).catch((error) => {
						// 			console.log("mail errrr 111", error)
						// 		});
						// 	} catch (err) {
						// 		throw err
						// 	}
						// });
						// //create mail history
						// let obj = {
						// 	project_id: project_id,
						// 	users: `${JSON.stringify(emails)}`,
						// 	mail_status: mail_status
						// }

						// await create_mail_history(obj)
						// return
					}
				}
				if (action === 'arrived') {
					mail_status = 4;
					subject = `Arrival Itreon project - ${project_id}`
					body = `<table style="font-size: 14px;width:100%"><tr><div><span>Hi,</span><br/><br/>Your devices from project <strong>${project_id}</strong>  for client <strong>${client_name}</strong> have now arrived ${arrived_time} at our production facility. We would also like to remind you that if there are still any locked devices, they should be unlocked as soon as possible.<br/><br/>If you have any other questions please contact us immediately.</div></tr></table>`;
					html = `${body}`
					content = `<br/><br/>${html}`
				}
				content = `${content}<br/><br/>${footer}`;
				// emails = ["anandsdn@gmail.com"]
				emails.forEach(async (email) => {
					try {
						let mailObj = {
							to: [email],
							subject: subject,
							html: content
						}
						if (action === 'create') {
							mailObj.attachments = [
								{
									path: __dirname + '/Client_information_new.pdf',
									filename: 'Client_information_new.pdf',
									contentType: 'contentType'
								}]
						}

						await mailService.send(mailObj).catch((error) => {
							console.log(project_id, "<=====Mail not sent project id==>", error)
						});
						let obj = {
							project_id: project_id,
							users: `${JSON.stringify(email)}`,
							mail_status: mail_status
						}

						await create_mail_history(obj)
					} catch (err) {
						console.log("<=====Mail not sent errr==>", err)

						throw err
					}
				});

			}
		} catch (error) {
			console.log("error 111", error)
			// throw new ServiceUnavailableException(error);
		}
	}

	async function create_mail_history(obj) {
		await mail_historyservice.createOne(
			obj
		).then((response1) => {
			// res.json(response);
			console.log("mail histtory created")
		}).catch((error1) => {
			console.log("mail histtory created error", error1)
		});
	}

	async function callTargetPrice(field = null, isSingle = false) {
		let cond = ''
		let cond11 = ''
		let cond2 = ''
		let cond22 = ''
		if (isSingle) {
			let model = '';
			let model1 = '';
			let processor = '';
			let processor1 = '';
			let grade = '';
			let grade1 = '';
			let memory = '';
			let memory1 = '';
			let Part_No = '';
			let Part_No1 = '';
			// -------------------------------------------------------------fetching in mobile data

			if (field.processor) {
				removeprocessorData.forEach((removable) => {
					field.processor = field.processor.toUpperCase().replace(removable.toUpperCase(), "").trim();
				})
				field.processor = field.processor ? field.processor.toUpperCase() : ''
			}
			if (field.model) {
				removeModelData.forEach((removable) => {
					field.model = field.model.toUpperCase().replace(removable.toUpperCase(), "").trim();
				})
			}
			// console.log("fields", field)
			if (field.model) {
				model = `UPPER(model) like '${field.model}'`
				model1 = `UPPER(t.model) like '${field.model}'`
			} else {
				model = `(model is null OR model = '' OR model like '')`
				model1 = `(t.model is null OR t.model ='')`
			}
			if (field.processor) {
				processor = `UPPER(processor) like '${field.processor}'`
				processor1 = `UPPER(t.processor) like '${field.processor}'`
			} else {
				processor = `(processor is null OR processor = '' OR processor like '')`
				processor1 = `(t.processor is null OR t.processor = '')`
			}
			if (field.grade) {
				grade = `UPPER(grade) = '${field.grade}'`
				grade1 = `UPPER(t.grade) = '${field.grade}'`
			} else {
				grade = `(grade is null OR grade = '' OR grade like '')`
				grade1 = `(t.grade is null OR t.grade = '')`
			}
			cond = `and ${model} and ${processor} and ${grade}`;
			cond11 = `and ${model1} and ${processor1} and ${grade1}`;
			// -------------------------------------------------------------fetching not in mobile data
			if (field.memory) {
				memory = `memory = '${field.memory}'`
				memory1 = `t.memory = '${field.memory}'`
			} else {
				memory = `(memory is null OR memory = '')`
				memory1 = `(t.memory is null OR t.memory = '')`
			}
			if (field.Part_No) {
				Part_No = `Part_No = '${field.Part_No}'`
				Part_No1 = `t.Part_No = '${field.Part_No}'`
			} else {
				Part_No = `(Part_No is null OR Part_No = '')`
				Part_No1 = `(t.Part_No is null OR t.Part_No = '')`
			}
			cond2 = `and ${model} and ${processor} and ${grade} and ${memory} and ${Part_No}`;
			cond22 = `and ${model1} and ${processor1} and ${grade1} and ${memory1} and ${Part_No1}`;
		}



		//Update processor ghz text


		if (isSingle) {
			let sql11 = `select UPPER(asset_type) as asset_type,UPPER(model) as model,UPPER(processor) as processor, UPPER(grade) as grade, MIN(sold_price) as min_sales,MAX(sold_price) as max_sales, count(*) sold_qty, sum(sold_price) tot_soldprice from public."Assets" where UPPER(asset_type) is not null and UPPER(asset_type)  in ('MONITOR' ,'COMPUTER' ,'MOBILE DEVICE') and UPPER(status) in ('SOLD' ,'RESERVATION') and (date_nor is not null) and date_nor::date >  CURRENT_DATE - INTERVAL '2 months' ${cond} and model !='' and model is not null and sold_price is not null group by UPPER(asset_type),UPPER(grade),UPPER(model),UPPER(processor)`
			//let sql1 = `select UPPER(asset_type) as asset_type,UPPER(model) as model,UPPER(processor) as processor, UPPER(grade) as grade, MIN(sold_price) as min_sales,MAX(sold_price) as max_sales, count(*) sold_qty, sum(sold_price) tot_soldprice from public."Assets" where (UPPER(asset_type) = 'MONITOR' OR UPPER(asset_type) = 'COMPUTER' or UPPER(asset_type) = 'MOBILE DEVICE') and (UPPER(status) like 'SOLD' OR UPPER(status) like 'RESERVATION') and  date_nor != '' and date_nor::date >  CURRENT_DATE - INTERVAL '6 months' group by UPPER(asset_type),UPPER(grade),UPPER(model),UPPER(processor)`
			// console.log("sqlllllllllll", sql11)
			await database.raw(sql11)
				.then(async (response) => {
					let result = response.rows;

					if (result?.length > 0) {

						result.forEach((item) => {
							let avg_prvice = (item.tot_soldprice / item.sold_qty)
							let percentage = Math.round((avg_prvice / 100) * 20)
							let target_price = avg_prvice + percentage
							item["target_price"] = Math.round(target_price)
						})
						updateDataFirst(result, isSingle, field?.asset_id)
					}
				})
				.catch((error) => {
					console.log("target price update error", error)
				});

		} else {
			// let sql2 = `update public."Assets" as t
			// set target_price = ROUND((s.tot_soldprice / s.sold_qty) + (((s.tot_soldprice::float/s.sold_qty)/100)*20)),
			// processor_bk = s.processor	
			// from (select UPPER(asset_type) as asset_type,UPPER(model) as model,UPPER(processor) as processor, UPPER(grade) as grade, MIN(sold_price) as min_sales,MAX(sold_price) as max_sales, count(*) sold_qty, sum(sold_price) tot_soldprice from public."Assets" where UPPER(asset_type) is not null and UPPER(asset_type)  in ('MONITOR' ,'COMPUTER' ,'MOBILE DEVICE') and UPPER(status) in ('SOLD' ,'RESERVATION') and (date_nor != '' and date_nor is not null and date_nor != 'Invalid date') and date_nor::date >  CURRENT_DATE - INTERVAL '2 months' ${cond}  group by UPPER(asset_type),UPPER(grade),UPPER(model),UPPER(processor)) s
			// where t.asset_type =s.asset_type ${cond} and UPPER(t.status) in ('IN STOCK','NOT ERASED','TEMP')`
			// await database.raw(sql2)
			// 	.then(async (response) => {
			// 		console.log("MONITOR,COMPUTER,MOBILE DEVICE, target price updated")
			// 	})
			// 	.catch((error) => {
			// 		console.log("MONITOR,COMPUTER,MOBILE DEVICE, target price updated error")
			// 	});
		}


		if (isSingle) {
			let sql2 = `select "Part_No", memory,UPPER(asset_type) as asset_type,UPPER(model) as model,UPPER(processor) as processor, UPPER(grade) as grade, MIN(sold_price) as min_sales,MAX(sold_price) as max_sales, count(*) sold_qty, sum(sold_price) tot_soldprice from public."Assets" where UPPER(asset_type) is not null and UPPER(asset_type) not in ('MONITOR' ,'COMPUTER' ,'MOBILE DEVICE') and UPPER(status) in ('SOLD' ,'RESERVATION') and (date_nor is not null) and date_nor::date >  CURRENT_DATE - INTERVAL '2 months' ${cond2} and model !='' and model is not null and sold_price is not null group by UPPER(asset_type),UPPER(grade),UPPER(model),UPPER(processor),"Part_No",memory`
			await database.raw(sql2)
				.then(async (response) => {
					let result = response.rows;
					if (result?.length > 0) {
						result.forEach((item) => {
							let avg_prvice = (item.tot_soldprice / item.sold_qty)
							let percentage = Math.round((avg_prvice / 100) * 20)
							let target_price = avg_prvice + percentage
							item["target_price"] = Math.round(target_price)
						})
						updateDataSecond(result, isSingle, field.asset_id)

					}
				})
				.catch((error) => {

				});

		} else {
			// let sql2 = `update public."Assets" as t
			// 			set target_price = ROUND((s.tot_soldprice / s.sold_qty) + (((s.tot_soldprice::float/s.sold_qty)/100)*20)),
			// 			processor_bk = s.processor	
			// 			from (select "Part_No", memory,UPPER(asset_type) as asset_type,UPPER(model) as model,UPPER(processor) as processor, UPPER(grade) as grade, MIN(sold_price) as min_sales,MAX(sold_price) as max_sales, count(*) sold_qty, sum(sold_price) tot_soldprice from public."Assets" where sold_price is not null and UPPER(asset_type) is not null and grade !='' and UPPER(asset_type) not in ('MONITOR' ,'COMPUTER' ,'MOBILE DEVICE') and UPPER(status) in ('SOLD' ,'RESERVATION') and (date_nor != '' and date_nor is not null and date_nor != 'Invalid date') and date_nor::date >  CURRENT_DATE - INTERVAL '2 months'  ${cond2} group by UPPER(asset_type),UPPER(grade),UPPER(model),UPPER(processor),"Part_No",memory) s
			// 			where t.asset_type =s.asset_type ${cond22} and UPPER(t.status) in ('IN STOCK','NOT ERASED','TEMP')`
			// await database.raw(sql2)
			// 	.then(async (response) => {
			// 		console.log("not in MONITOR,COMPUTER,MOBILE DEVICE, target price updated")
			// 	})
			// 	.catch((error) => {
			// 		console.log("not in MONITOR,COMPUTER,MOBILE DEVICE, target price updated", error)
			// 	});
		}






	}
	async function updateDataSecond(data, isSingle, asset_id) {
		if (data) {
			if (isSingle) {
				let field = data[0];
				return await assetsService.updateOne(asset_id,
					{
						target_price: field.target_price

					}
				).then((response1) => {
					// res.json(response);
					console.log("target updateee success", response1)
				}).catch((error1) => {
					console.log("target updateee errrrr", field.asset_id)
				});

				// let sql = `update public."Assets" set target_updated='updated',target_price = ${field.target_price},date_updated = '${date.format('YYYY-MM-DD HH:MM')}' where asset_id = ${asset_id}`
				// await database.raw(sql).then(async (res) => {
				// 	console.log("suxsss target ", sql)
				// }).catch((error) => {
				// 	console.log("target updateee", error)
				// });
			}
			// else {
			// 	data.map(async (field) => {
			// 		let model
			// 		let processor
			// 		let grade
			// 		let memory
			// 		let Part_No

			// 		if (field.model) {
			// 			model = `UPPER(model) like '${field.model}'`
			// 		} else {
			// 			model = `(model is null OR model ='')`
			// 		}
			// 		if (field.processor) {
			// 			processor = `UPPER(processor) like '${field.processor}'`
			// 		} else {
			// 			processor = `(processor is null OR processor = '')`
			// 		}
			// 		if (field.grade) {
			// 			grade = `UPPER(grade) = '${field.grade}'`
			// 		} else {
			// 			grade = `(grade is null OR grade = '')`
			// 		}
			// 		if (field.memory) {
			// 			memory = `memory = '${field.memory}'`
			// 		} else {
			// 			memory = `(memory is null OR memory = '')`
			// 		}
			// 		if (field.Part_No) {
			// 			Part_No = `Part_No = '${field.Part_No}'`
			// 		} else {
			// 			Part_No = `(Part_No is null OR Part_No = '')`
			// 		}
			// 		let sql = `update public."Assets" set target_price = ${field.target_price},date_updated = '${date.format('YYYY-MM-DD')}' where UPPER(asset_type)='${field.asset_type}' and ${model} and ${processor} and ${grade} and ${Part_No} and ${memory} and UPPER(status) in ('IN STOCK','NOT ERASED','TEMP')`
			// 		// return
			// 		await database.raw(sql).then(async (res) => {
			// 			console.log("update target second target ", field.target_price)
			// 		}).catch((error) => {
			// 			console.log("error target second target ", error)
			// 		});
			// 	})
			// }
		}
	}

	async function updateDataFirst(data, isSingle, asset_id) {
		if (data) {
			if (isSingle) {
				let field = data[0];
				// return await assetsService.updateOne(asset_id,
				// 	{
				// 		target_price: field.target_price

				// 	}
				// ).then((response1) => {
				// 	// res.json(response);
				// 	console.log("target updateee success", response1)
				// }).catch((error1) => {
				// 	console.log("target updateee errrrr", field.asset_id)
				// });

				let sql = `update public."Assets" set target_price = ${field.target_price},date_updated = '${date.format('YYYY-MM-DD HH:MM')}' where asset_id = ${asset_id} and model !='' and model is not null`
				await database.raw(sql).then(async (res) => {
					console.log("sucess target")
				}).catch((error) => {
					console.log("target updateee", error)
				});

			}
			//  else {
			// 	data.map(async (field) => {
			// 		let model
			// 		let processor
			// 		let grade

			// 		if (field.model) {
			// 			model = `model like '${field.model}'`
			// 		} else {
			// 			model = `(model is null OR model ='')`
			// 		}
			// 		if (field.processor) {
			// 			processor = `processor like '${field.processor}'`
			// 		} else {
			// 			processor = `(processor is null OR processor = '')`
			// 		}
			// 		if (field.grade) {
			// 			grade = `grade = '${field.grade}'`
			// 		} else {
			// 			grade = `(grade is null OR grade = '')`
			// 		}
			// 		let sql = `update public."Assets" set target_price = ${field.target_price},date_updated = '${date.format('YYYY-MM-DD')}' where UPPER(asset_type)='${field.asset_type}' and ${model} and ${processor} and ${grade} and UPPER(status) in ('IN STOCK','NOT ERASED','TEMP')`
			// 		await database.raw(sql).then(async (res) => {
			// 			console.log(sql, "update target firsrt target ", res)
			// 		}).catch((error) => {
			// 			console.log("error target first target ", error)
			// 		});
			// 	})
			// }
		}
	}

	//update partner commission for project commission percentage 

	async function updatePartnerCommission(data) {
		if (!data.partner) {
			return
		}
		const partnerData = await partner_service.readByQuery({
			fields: ["commission"],
			filter: {
				id: {
					_eq: data.partner
				}
			},
		});

		if (partnerData.length > 0 && partnerData[0]?.commission) {
			return await projectService.updateOne(data.id,
				{
					commision_percentage: partnerData[0]?.commission
				}
			).then((response1) => {
				// res.json(response);
				console.log("project commission updateee success", response1)
			}).catch((error1) => {
				console.log("project commission updateee errrrr", field.asset_id)
			});
		}
	}
};
