#pragma once

#include <algorithm>
#include <atomic>
#include <ppl.h>
#include <ppltasks.h>
#include <mutex>
#include "Utils.h"

namespace MTObjects
{
template<typename T> using FastContainer = SmartStack<T>;
using IndexSet = std::bitset<128>;

class IThreadSafeObject
{
public:
#if 0
	TIndex cluster_index_ = kNullIndex;
#else
	std::atomic_ushort cluster_index_ = { kNullIndex };
#endif

	TIndex GetClusterIndex() const { return cluster_index_; }
	void SetClusterIndex(TIndex index) { cluster_index_ = index; }

	virtual void IsDependentOn(FastContainer<IThreadSafeObject*>& ref_dependencies) const = 0;
	virtual void IsConstDependentOn(IndexSet& ref_dependencies) const = 0;

	virtual void Task() = 0;
};

struct Cluster
{
private:
	FastContainer<IThreadSafeObject*> objects_;
#pragma region default_stuff
public:
	Cluster() = default;
	~Cluster() = default;
	Cluster(Cluster&& other) = default;
	Cluster& operator=(Cluster&& other) = default;
	Cluster(const Cluster&) = delete;
	Cluster& operator=(const Cluster&) = delete;

	FastContainer<IThreadSafeObject*>& GetObjects() { return objects_; }
	const FastContainer<IThreadSafeObject*>& GetObjects() const { return objects_; }
	void Reset() { GetObjects().clear(); }
#pragma endregion
public:
	static void CreateClusters(const vector<IThreadSafeObject *> &all_objects, vector<Cluster>& clusters)
	{
		for(unsigned int first_remaining_obj_index = 0; first_remaining_obj_index < all_objects.size(); first_remaining_obj_index++)
		{
			IThreadSafeObject* const initial_object = all_objects[first_remaining_obj_index];
			if(kNullIndex != initial_object->GetClusterIndex())
				continue;

			TIndex cluster_index = static_cast<TIndex>(clusters.size());
			Cluster* const initial_cluster = &clusters.emplace_back();
			IF_TEST_STUFF(TestStuff::max_num_clusters() = std::max(TestStuff::max_num_clusters(), static_cast<unsigned int>(clusters.size()) ));
			Cluster* actual_cluster = initial_cluster;
			FastContainer<IThreadSafeObject*> objects_to_handle;
			objects_to_handle.push_back(initial_object);
			while (!objects_to_handle.empty())
			{
				IThreadSafeObject* obj = objects_to_handle.back();
				objects_to_handle.pop_back();
				const TIndex cluster_of_object = obj->GetClusterIndex();
				if (kNullIndex == cluster_of_object)
				{
					actual_cluster->GetObjects().push_back(obj);
					obj->SetClusterIndex(cluster_index);
					obj->IsDependentOn(objects_to_handle);
					IF_TEST_STUFF(TestStuff::max_num_objects_to_handle() = std::max(TestStuff::max_num_objects_to_handle(), objects_to_handle.size()));
				}
				else if (cluster_of_object != cluster_index)
				{
					const bool use_new_cluster = clusters[cluster_of_object].GetObjects().size() > actual_cluster->GetObjects().size();
					Cluster& to_merge = clusters[use_new_cluster ? cluster_index : cluster_of_object];
					cluster_index = use_new_cluster ? cluster_of_object : cluster_index;
					actual_cluster = &clusters[cluster_index];
					for (auto object_merged : to_merge.GetObjects())
					{
						object_merged->SetClusterIndex(cluster_index);
					}
					ContainerFunc::Merge(actual_cluster->GetObjects(), to_merge.GetObjects());
					if (&to_merge == initial_cluster)
					{
						clusters.pop_back();
					}
				}
			}
		}
	}

	static void CreateClustersMT(const vector<IThreadSafeObject *> &all_objects, vector<Cluster>& clusters)
	{
		struct ThreadData
		{
			std::mutex mutex;
			FastContainer<IThreadSafeObject*> objects_to_handle;
			FastContainer<IThreadSafeObject*> objects_to_store_in_cluster;
			std::atomic_ushort redirected_index = { kNullIndex }; //changed only once, no chains of redirection
			bool being_processed = false;
		}; 

		std::vector<ThreadData> cluster_threads(64 * 1024);
		std::atomic_ushort num_filled_clusters = 0;
		std::atomic_uint first_remaining_obj_index = 0;
		auto create_cluster = [&]() -> bool 
		{
			unsigned short cluster_index = num_filled_clusters++;
			{
				const auto num_objects = all_objects.size();
				TIndex loc_null_idx = kNullIndex;
				unsigned int initial_obj_idx = 0;
				ThreadData& context = cluster_threads[cluster_index];
				std::lock_guard<std::mutex> lock(context.mutex);
				Assert(!context.being_processed);
				do{
					initial_obj_idx = first_remaining_obj_index++;
					if (initial_obj_idx >= num_objects)
						return false;
					loc_null_idx = kNullIndex;
				} while (!all_objects[initial_obj_idx]->cluster_index_.compare_exchange_strong(loc_null_idx, cluster_index));
				context.being_processed = true;
				IThreadSafeObject* initial_obj = all_objects[initial_obj_idx];
				context.objects_to_store_in_cluster.push_back(initial_obj);
				initial_obj->IsDependentOn(context.objects_to_handle);
			}

			while (true)
			{
				TIndex obj_cluster_index = kNullIndex;
				{
					ThreadData& context = cluster_threads[cluster_index];
					std::lock_guard<std::mutex> lock(context.mutex);
					if (context.objects_to_handle.empty())
					{
						context.being_processed = false;
						return true;
					}
					Assert(context.being_processed);
					IThreadSafeObject* obj = context.objects_to_handle.back();
					Assert(obj);
					const bool was_unassigned = obj->cluster_index_.compare_exchange_strong(obj_cluster_index, cluster_index);
					if (was_unassigned)
					{
						context.objects_to_handle.pop_back();
						context.objects_to_store_in_cluster.push_back(obj);
						obj->IsDependentOn(context.objects_to_handle);
						continue;
					}
				}

				{
					auto other_cluster_index = obj_cluster_index;
					for (TIndex redirected = cluster_threads[other_cluster_index].redirected_index
						; kNullIndex != redirected
						; redirected = cluster_threads[other_cluster_index].redirected_index)
					{
						other_cluster_index = redirected;
					}
					if (other_cluster_index != cluster_index)
					{
						const bool use_other_cluster = other_cluster_index < cluster_index;
						ThreadData& to_merge = cluster_threads[use_other_cluster ? cluster_index : use_other_cluster];
						ThreadData& to_survive = cluster_threads[use_other_cluster ? use_other_cluster : cluster_index];
						
						std::lock_guard<std::mutex> lock_smaller(to_survive.mutex);
						std::lock_guard<std::mutex> lock_bigger(to_merge.mutex);

						if (!cluster_threads[cluster_index].being_processed)
							return true;
						cluster_threads[cluster_index].objects_to_handle.pop_back();

						const auto survive_index = use_other_cluster ? other_cluster_index : cluster_index;
						to_merge.redirected_index = survive_index;
						to_merge.being_processed = false;
						//for (auto object_merged : to_merge.objects_to_store_in_cluster)
						//{
						//	object_merged->SetClusterIndex(survive_index);
						//}
						ContainerFunc::Merge(to_survive.objects_to_store_in_cluster, to_merge.objects_to_store_in_cluster);
						ContainerFunc::Merge(to_survive.objects_to_handle, to_merge.objects_to_handle);
						if (use_other_cluster && !to_survive.being_processed)
						{
							cluster_index = other_cluster_index;
							to_survive.being_processed = true;
						}
					}
					else
					{
						ThreadData& context = cluster_threads[cluster_index];
						std::lock_guard<std::mutex> lock(context.mutex);
						if (!context.being_processed)
							return true;
						context.objects_to_handle.pop_back();
					}
				}
			}
		};

		while (create_cluster());

#pragma region run_parallelly
		/*
		auto create_many_clusters = [&]() 
		{
			while (create_cluster());
		};
		const unsigned int kNumConcurrentTasks = 8;
		std::array<concurrency::task<void>, kNumConcurrentTasks> tasks;
		for (int i = 0; i < kNumConcurrentTasks; i++)
		{
			tasks[i] = concurrency::create_task(create_many_clusters);
		}
		auto join_task = when_all(begin(tasks), end(tasks));
		join_task.wait();
		*/
#pragma endregion

		for (auto& thread_data : cluster_threads)
		{
			Assert(!thread_data.being_processed);
			Assert(thread_data.objects_to_handle.empty());
			Assert((thread_data.redirected_index != kNullIndex) == thread_data.objects_to_store_in_cluster.empty());
			if (!thread_data.objects_to_store_in_cluster.empty())
			{
				const TIndex cluster_index = static_cast<TIndex>(clusters.size());
				clusters.emplace_back();
				for (auto object_merged : thread_data.objects_to_store_in_cluster)
				{
					object_merged->SetClusterIndex(cluster_index);
				}
				clusters.back().objects_ = std::move(thread_data.objects_to_store_in_cluster);
			}
		}
	}

	static vector<IndexSet> CreateClustersDependencies(vector<Cluster>& clusters)
	{
		const auto num_clusters = clusters.size();
		vector<IndexSet> const_dependencies_clusters;
		const_dependencies_clusters.resize(num_clusters);
		
		concurrency::parallel_for<size_t>(0, num_clusters, [&clusters, &const_dependencies_clusters](size_t idx)
		{
			auto& cluster = clusters[idx];
			auto& const_dependency_set = const_dependencies_clusters[idx];
			for (auto obj : cluster.GetObjects())
			{
				Assert(obj->GetClusterIndex() == idx);
				obj->IsConstDependentOn(const_dependency_set);
			}
			const_dependency_set[idx] = false;
		});

		return const_dependencies_clusters;
	}

#ifdef TEST_STUFF 
	static bool Test_AreClustersCoherent(const vector<Cluster>& clusters)
	{
		// All dependencies of the objects must be inside the cluster
		for (int idx = 0; idx < clusters.size(); idx++)
		{
			auto& cluster = clusters[idx];
			auto& objects = cluster.GetObjects();
			for (auto obj : objects)
			{
				Assert(obj && obj->GetClusterIndex() == idx);

				FastContainer<IThreadSafeObject*> dependencies;
				obj->IsDependentOn(dependencies);
				for (auto dep : dependencies)
				{
					Assert(dep && dep->GetClusterIndex() == idx);
				}
			}
			vector<IThreadSafeObject*> objects_vec(objects.begin(), objects.end());
			std::sort(objects_vec.begin(), objects_vec.end());
			for (auto iter = objects_vec.begin(); !objects_vec.empty();)
			{
				auto obj = *iter;
				iter++;
				if (iter == objects_vec.end())
					break;
				Assert(obj != *iter);
			}
		}
		return true;
	}
#endif //TEST_STUFF
};

static_assert(sizeof(Cluster) == sizeof(FastContainer<IThreadSafeObject*>));

struct GroupOfConcurrentClusters
{
	vector<Cluster*> clusters_;
	IndexSet clusters_in_group_;
	IndexSet const_dependencies_clusters_;

	void AddCluster(Cluster& cluster, TIndex cluster_index, const IndexSet& cluster_dependencies)
	{
		clusters_.emplace_back(&cluster);
		const_dependencies_clusters_ |= cluster_dependencies;
		clusters_in_group_[cluster_index] = true;
	}

public:
	static vector<GroupOfConcurrentClusters> GenerateClusterGroups(vector<Cluster>& clusters, const vector<IndexSet>& dependency_sets)
	{
		vector<GroupOfConcurrentClusters> groups;
		groups.reserve(clusters.size()/4);
		groups.resize(1); //groups.resize(std::max<size_t>(1, clusters.size() / 32));
		for (TIndex cluster_index = 0; cluster_index < clusters.size(); ++cluster_index)
		{
			auto cluster = &clusters[cluster_index];
			auto& dependency_set = dependency_sets[cluster_index];
			bool fits_in_existing_group = false;
			{
				const size_t group_num = groups.size();
				const size_t first_group_to_try = cluster_index % group_num;
				for (size_t groups_checked = 0; groups_checked < group_num && !fits_in_existing_group; groups_checked++)
				{
					const size_t group_idx = (first_group_to_try + groups_checked) % group_num;
					auto& group = groups[group_idx];
					const bool cluster_depends_on_group = (group.clusters_in_group_ & dependency_set).any();
					const bool grup_depends_on_cluster = group.const_dependencies_clusters_[cluster_index];
					if (!cluster_depends_on_group && !grup_depends_on_cluster)
					{
						group.AddCluster(*cluster, cluster_index, dependency_set);
						fits_in_existing_group = true;
					}
				}
			}
			if (!fits_in_existing_group)
			{
				GroupOfConcurrentClusters new_group;
				new_group.AddCluster(*cluster, cluster_index, dependency_set);
				groups.emplace_back(new_group);
			}
		}
		return groups;
	}

	void ExecuteGroup()
	{
		concurrency::parallel_for_each(clusters_.begin(), clusters_.end(), [](Cluster* cluster)
		{
			for (auto obj : cluster->GetObjects())
			{
				obj->Task();
				obj->SetClusterIndex(kNullIndex);
			}
			cluster->Reset();
		});
	}
};
}
